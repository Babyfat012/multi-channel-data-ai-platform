"""Check the producer/oracle and independent reconciliation, without Kafka or Docker."""

import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest

from generator.build_order_lifecycle_stream_oracle import expected
from generator.generate_order_lifecycle_fixture import generate_fixture
from generator.publish_order_lifecycle_events import FileSink, encoded, run_stream
from scripts.evidence.compare_order_lifecycle_stream import compare
from scripts.run.render_order_lifecycle_stream import render


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    """Generate the real observed fixture once for the stream test group."""
    root = tmp_path_factory.mktemp("stream-source") / "source"
    generate_fixture(root)
    return root


def produce(source, root, epoch=1, rate=0.03, seed=7):
    """Run the file sink and return the receipts with the actually emitted rows."""
    manifest = root / f"manifest-{epoch}.json"
    log = root / f"log-{epoch}.jsonl"
    result = run_stream(
        source, FileSink(log), manifest, epoch=epoch, duplicate_rate=rate, seed=seed
    )
    return result, [json.loads(line) for line in log.read_text().splitlines()], manifest


def test_order_retry_seed_revisions_and_epoch(source, tmp_path):
    """Retries keep the exact bytes/hash; revisions stay distinct; replay only changes the epoch header."""
    first, rows, path = produce(source, tmp_path)
    second, replay, _ = produce(source, tmp_path, epoch=2)
    assert [r["value"]["source_observed_at"] for r in rows] == sorted(
        r["value"]["source_observed_at"] for r in rows
    )
    counts = Counter(r["value"]["record_sha256"] for r in rows)
    assert sum(n - 1 for n in counts.values()) == int(len(counts) * 0.03)
    assert first["ledger"] == second["ledger"]
    assert first["sha256"] == second["sha256"]
    assert all(r["headers"]["epoch"] == "1" for r in rows)
    assert all(r["headers"]["epoch"] == "2" for r in replay)
    assert [r["value"] for r in rows] == [r["value"] for r in replay]
    revisions = {}
    for r in rows:
        value = r["value"]
        revisions.setdefault(value["entity_id"], set()).add(value["record_sha256"])
    assert max(map(len, revisions.values())) >= 3
    for sha, n in counts.items():
        if n > 1:
            assert (
                len({encoded(r["value"]) for r in rows if r["value"]["record_sha256"] == sha}) == 1
            )
    oracle = expected(source, path)
    assert all(r["main"] + r["late"] == r["input"] for r in oracle["late"])
    assert sum(r["dup_removed"] for r in oracle["dedup"]) == sum(n - 1 for n in counts.values())
    assert sum(r["revisions_kept"] for r in oracle["dedup"]) > 0
    assert sum(r["late"] for r in oracle["late"]) > 0


def tiny_source(root):
    """Hand-build approved/requested/refund failed/shipping/correction to count independently."""
    root.mkdir()
    cases = [
        (
            "seller_postsales.return_events",
            "return",
            1,
            "2026-01-02",
            "2026-01-02",
            {"event_type": "requested", "quantity": 1},
        ),
        (
            "seller_postsales.return_events",
            "return",
            2,
            "2026-01-03",
            "2026-01-03",
            {"event_type": "approved", "quantity": 1},
        ),
        (
            "seller_postsales.return_events",
            "late",
            1,
            "2026-01-01",
            "2026-01-04",
            {"event_type": "approved", "quantity": 1},
        ),
        (
            "seller_postsales.refund_events",
            "refund",
            1,
            "2026-01-01",
            "2026-01-01",
            {"status": "failed", "component": "product", "amount_minor": 5},
        ),
        (
            "seller_postsales.refund_events",
            "refund",
            2,
            "2026-01-02",
            "2026-01-02",
            {"status": "success", "component": "product", "amount_minor": 5},
        ),
        (
            "seller_postsales.refund_events",
            "shipping",
            1,
            "2026-01-02",
            "2026-01-02",
            {"status": "success", "component": "shipping", "amount_minor": 2},
        ),
        (
            "seller_postsales.refund_events",
            "refund",
            3,
            "2026-01-02",
            "2026-01-05",
            {"status": "success", "component": "product", "amount_minor": 6},
        ),
    ]
    files = []
    for i, (feed, entity, revision, event, known, payload) in enumerate(cases):
        row = dict(
            feed=feed,
            entity_id=entity,
            entity_revision=revision,
            source_sequence=i,
            source_event_id=str(i),
            dataset_version="tiny",
            effective_at=event + "T00:00:00.000000Z",
            source_observed_at=known + "T00:00:00.000000Z",
            payload=payload,
        )
        row["record_sha256"] = hashlib.sha256(encoded(row)).hexdigest()
        data = encoded(row) + b"\n"
        name = f"{i}.jsonl"
        (root / name).write_bytes(data)
        files.append(
            dict(path=name, feed=feed, role="source", sha256=hashlib.sha256(data).hexdigest())
        )
    (root / "manifest.json").write_text(json.dumps(dict(dataset_version="tiny", files=files)))
    return root


def test_hand_built_windows_and_late(tmp_path):
    """Hand-computed expectation: two approvals and two successful refund revisions; retries do not increase the window."""
    source = tiny_source(tmp_path / "source")
    receipt, _, path = produce(source, tmp_path, rate=1)
    oracle = expected(source, path, delay_seconds=3600)
    windows = {(r["day"], r["metric"]): r["events"] for r in oracle["windows"]}
    assert windows == {
        ("2026-01-01", "approved_return_events"): 1,
        ("2026-01-03", "approved_return_events"): 1,
        ("2026-01-02", "successful_product_refund_events"): 2,
    }
    assert sum(r["dup_removed"] for r in oracle["dedup"]) == 7
    assert (
        receipt["topics"]["order_lifecycle.seller_postsales.return_events"]["injected_duplicates"]
        == 3
    )
    assert next(r for r in oracle["late"] if r["topic"].endswith("return_events"))["late"] == 2


def test_oracle_rejects_tampered_receipt(source, tmp_path):
    """Never trust a manifest with tampered offsets/counters/digests as the answer key."""
    manifest, _, path = produce(source, tmp_path)
    manifest["ledger"][0]["record_sha256"] = manifest["ledger"][1]["record_sha256"]
    path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, KeyError)):
        expected(source, path)


def write_rows(root, relative, rows):
    """Write fake output in the Flink schema to check that the comparator detects errors."""
    path = root / relative / "part-0"
    path.parent.mkdir(parents=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def test_comparator_detects_loss_duplicates_and_missing_epoch(source, tmp_path):
    """Correct output must pass; missing late, leaked retries and wrong epochs must all fail."""
    _, _, path = produce(source, tmp_path)
    oracle = expected(source, path, delay_seconds=0)
    output = tmp_path / "output"
    unique = {r["record_sha256"]: r for r in reversed(oracle["records"])}
    write_rows(output, "baseline/1/counts", oracle["baseline"])
    write_rows(output, "late/1/routes", oracle["records"])
    write_rows(output, "dedup/1/input", oracle["records"])
    write_rows(output, "dedup/1/kept", list(unique.values()))
    write_rows(output, "dedup/1/counts", oracle["dedup"])
    daily = []
    for r in oracle["windows"]:
        row = dict(
            topic=r["topic"],
            epoch=1,
            day=r["day"],
            approved_return_events=0,
            successful_product_refund_events=0,
        )
        row[r["metric"]] = r["events"]
        daily.append(row)
    write_rows(output, "windows/1/daily", daily)
    assert compare(output, oracle)["passed"]
    part = output / "late/1/routes/part-0"
    original = part.read_text()
    part.write_text("\n".join(original.splitlines()[1:]))
    assert not compare(output, oracle)["checks"]["main_plus_late_equals_input"]
    part.write_text(original)
    part = output / "dedup/1/kept/part-0"
    part.write_text(part.read_text() + json.dumps(next(iter(unique.values()))) + "\n")
    assert not compare(output, oracle)["checks"]["dedup_keeps_every_revision_once"]
    assert not compare(output, dict(oracle, epoch=2))["passed"]


def test_render_and_validate(source, tmp_path):
    """Render the full checkpoint/epoch config and refuse to overwrite the template or output."""
    _, _, manifest = produce(source, tmp_path, epoch=12)
    render(tmp_path / "sql", epoch=12, manifest=manifest, delay_seconds=60, checkpoint_ms=250)
    files = list((tmp_path / "sql").glob("*.sql"))
    assert len(files) == 4
    assert all("@@" not in p.read_text() for p in files)
    assert all("'250ms'" in p.read_text() for p in files)
    with pytest.raises(FileExistsError):
        render(tmp_path / "sql", epoch=12, manifest=manifest)
    with pytest.raises(ValueError):
        render(tmp_path / "invalid", epoch=-1, manifest=manifest)
    with pytest.raises(ValueError):
        produce(source, tmp_path, epoch=-1)


@pytest.mark.parametrize("rate", [0, 0.17, 1])
def test_exact_rate_and_preserved_source_envelope(source, tmp_path, rate):
    """Check ratios and every provenance field; the fallback hash comes from the raw source without LF."""
    receipt, rows, path = produce(source, tmp_path, rate=rate, seed=42)
    manifest = json.loads((source / "manifest.json").read_text())
    originals = {}
    for item in manifest["files"]:
        if "order_lifecycle." + item["feed"] not in receipt["topics"]:
            continue
        for raw in (source / item["path"]).read_bytes().splitlines():
            original = json.loads(raw)
            originals[original.get("record_sha256", hashlib.sha256(raw).hexdigest())] = original
    for row in rows:
        value = dict(row["value"])
        sha = value["record_sha256"]
        if "record_sha256" not in originals[sha]:
            del value["record_sha256"]
        assert value == originals[sha]
        assert row["key"] == value["entity_id"]
    assert len(rows) - len(originals) == int(len(originals) * rate)
    oracle = expected(source, path)
    assert sum(r["input"] for r in oracle["baseline"]) == len(rows)


def test_retry_selection_varies_by_seed(source, tmp_path):
    """The seed changes the retry set but not the source envelope set."""
    first, _, _ = produce(source, tmp_path, rate=0.2, seed=1)
    second, _, _ = produce(source, tmp_path, epoch=2, rate=0.2, seed=2)
    selected1 = {r["record_sha256"] for r in first["ledger"] if r["retry"]}
    selected2 = {r["record_sha256"] for r in second["ledger"] if r["retry"]}
    assert len(selected1) == len(selected2)
    assert selected1 != selected2


def test_late_mismatch_one_record_fails(source, tmp_path):
    """Turning exactly one late into main still preserves the input but must FAIL."""
    _, _, path = produce(source, tmp_path)
    oracle = expected(source, path, delay_seconds=0)
    output = tmp_path / "output"
    rows = [dict(r) for r in oracle["records"]]
    write_rows(output, "late/1/routes", rows)
    assert compare(output, oracle)["checks"]["late_equals_oracle"]
    next(r for r in rows if r["route"] == "late")["route"] = "main"
    part = output / "late/1/routes/part-0"
    part.write_text("".join(json.dumps(r) + "\n" for r in rows))
    result = compare(output, oracle)
    assert result["checks"]["main_plus_late_equals_input"]
    assert not result["checks"]["late_equals_oracle"]
    assert not result["passed"]


def test_mixed_header_epoch_is_detected(source, tmp_path):
    """A record with the right offset/hash but a different epoch header must show up and cause a FAIL."""
    _, _, manifest = produce(source, tmp_path)
    oracle = expected(source, manifest)
    rows = [dict(r) for r in oracle["records"]]
    rows[0]["epoch"] = 2
    write_rows(tmp_path / "output", "late/1/routes", rows)
    result = compare(tmp_path / "output", oracle)
    assert not result["checks"]["headers_match_epoch"]
    assert not result["checks"]["main_plus_late_equals_input"]
    render(tmp_path / "sql", epoch=1, manifest=manifest)
    assert all(
        "WHERE CAST(kafka_headers['epoch']" not in p.read_text()
        for p in (tmp_path / "sql").glob("*.sql")
    )


def test_offset_boundaries_and_proof_render(source, tmp_path):
    """Replay only reads new-epoch offsets; each proof SQL has exactly one topic and is on-event."""
    manifest_path = tmp_path / "manifest.json"
    receipt = run_stream(
        source, FileSink(tmp_path / "log"), manifest_path, epoch=2, late_topics=True
    )
    proof_path = Path(receipt["late_manifest"])
    proof = expected(source, proof_path, delay_seconds=0)
    assert sum(r["late"] > 0 for r in proof["late"]) >= 2
    assert all(r["partition"] == 0 for r in proof["records"])
    # Simulate offsets already present from a previous epoch on the same broker.
    for entry in receipt["ledger"]:
        entry["offset"] += 123
    for stats in receipt["topics"].values():
        for values in stats["partitions"].values():
            values["first_offset"] += 123
            values["last_offset"] += 123
    manifest_path.write_text(json.dumps(receipt))
    render(tmp_path / "sql", epoch=2, manifest=manifest_path)
    for path in (tmp_path / "sql").glob("*.sql"):
        sql = path.read_text()
        assert "'scan.startup.mode' = 'specific-offsets'" in sql
        assert "'scan.bounded.mode' = 'specific-offsets'" in sql
        assert "'scan.startup.specific-offsets' = 'partition:0,offset:123'" in sql
        for stats in receipt["topics"].values():
            stop = stats["partitions"]["0"]["last_offset"] + 1
            assert f"'scan.bounded.specific-offsets' = 'partition:0,offset:{stop}'" in sql
        assert "earliest-offset" not in sql and "latest-offset" not in sql
    render(tmp_path / "proof", epoch=2, manifest=proof_path, late_topics=True)
    files = list((tmp_path / "proof").glob("*.sql"))
    assert len(files) == 4
    for path in files:
        sql = path.read_text()
        assert sql.count("'connector' = 'kafka'") == 1
        assert "'scan.watermark.emit.strategy' = 'on-event'" in sql
        assert "SET 'parallelism.default' = '1'" in sql
        assert "/order_lifecycle/late-proof/2/routes" in sql
    receipt["ledger"][0]["offset"] += 1
    manifest_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="ledger"):
        render(tmp_path / "bad", epoch=2, manifest=manifest_path)


@pytest.mark.parametrize(
    "field,value", [("first_offset", -1), ("last_offset", "1;DROP"), ("records", 0)]
)
def test_invalid_offset_format_rejected(source, tmp_path, field, value):
    """Receipts with negative offsets, SQL strings or gaps must not reach the template."""
    receipt, _, manifest = produce(source, tmp_path)
    next(iter(next(iter(receipt["topics"].values()))["partitions"].values()))[field] = value
    manifest.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        render(tmp_path / "sql", epoch=1, manifest=manifest)


def test_separate_late_proof_detects_single_error_and_epoch(source, tmp_path):
    """The separate proof branch cannot PASS when one late is off or epoch headers are mixed."""
    receipt = run_stream(
        source, FileSink(tmp_path / "log"), tmp_path / "manifest.json", epoch=1, late_topics=True
    )
    oracle = expected(source, tmp_path / "manifest.json", delay_seconds=0)
    proof = expected(source, receipt["late_manifest"], delay_seconds=0)
    output = tmp_path / "output"
    write_rows(output, "baseline/1/counts", oracle["baseline"])
    write_rows(output, "late/1/routes", oracle["records"])
    write_rows(output, "dedup/1/input", oracle["records"])
    unique = {r["record_sha256"]: r for r in reversed(oracle["records"])}
    write_rows(output, "dedup/1/kept", list(unique.values()))
    write_rows(output, "dedup/1/counts", oracle["dedup"])
    daily = []
    for row in oracle["windows"]:
        item = dict(
            topic=row["topic"],
            epoch=1,
            day=row["day"],
            approved_return_events=0,
            successful_product_refund_events=0,
        )
        item[row["metric"]] = row["events"]
        daily.append(item)
    write_rows(output, "windows/1/daily", daily)
    write_rows(output, "late-proof/1/routes", proof["records"])
    assert compare(output, oracle, proof)["passed"]
    rows = [dict(r) for r in proof["records"]]
    changed = next(r for r in rows if r["route"] == "late")
    changed["route"] = "main"
    part = output / "late-proof/1/routes/part-0"
    part.write_text("".join(json.dumps(r) + "\n" for r in rows))
    result = compare(output, oracle, proof)
    assert result["checks"]["late_proof_input_exact"]
    assert not result["checks"]["late_equals_oracle"]
    assert not result["passed"]
    changed["route"] = "late"
    changed["epoch"] = 2
    part.write_text("".join(json.dumps(r) + "\n" for r in rows))
    result = compare(output, oracle, proof)
    assert not result["checks"]["headers_match_epoch"]
    assert not result["passed"]


def test_restart_render_is_unbounded_with_separate_summary(source, tmp_path):
    """The restart case has no EOF; the bounded summary runs separately after cancelling streaming."""
    _, _, manifest = produce(source, tmp_path, epoch=2)
    render(tmp_path / "restart", epoch=2, manifest=manifest, restart_mode=True, checkpoint_ms=100)
    sql = (tmp_path / "restart/dedup.sql").read_text()
    assert "SET 'execution.runtime-mode' = 'streaming';" in sql
    assert sql.count("'scan.startup.mode' = 'specific-offsets'") == 4
    assert "scan.bounded" not in sql
    assert "lifecycle_count_source" not in sql
    assert "'execution.checkpointing.mode' = 'EXACTLY_ONCE'" in sql
    assert "'sink.rolling-policy.rollover-interval'='1 s'" in sql
    summary = (tmp_path / "restart/dedup_counts.sql").read_text()
    assert summary.index("SET 'execution.runtime-mode' = 'batch'") < summary.index("CREATE TABLE")
    assert summary.count("'scan.bounded.mode' = 'specific-offsets'") == 4
    assert "INSERT INTO dedup_counts" in summary
    assert "INSERT INTO kept" not in summary
    assert (
        "'scan.bounded.mode' = 'specific-offsets'"
        in (tmp_path / "restart/baseline.sql").read_text()
    )
    with pytest.raises(ValueError, match="restart"):
        render(
            tmp_path / "invalid-restart",
            epoch=2,
            manifest=manifest,
            restart_mode=True,
            late_topics=True,
        )


def load_restart_control():
    """Load the control harness as a module, without starting the CLI or Docker."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "restart_control",
        Path(__file__).resolve().parents[3] / "tests/harness/control_order_lifecycle_stream.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_restart_count_committed_output_only(tmp_path):
    """Count only the requested epoch in committed files; skip hidden, pending and metadata."""
    control = load_restart_control()
    root = tmp_path / "kept"
    write_rows(tmp_path, "kept/day=1", [dict(epoch=2), dict(epoch=1), dict(epoch=2)])
    for relative in (".part-1.inprogress", "_SUCCESS", ".staging/part-2", "_temporary/part-3"):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not committed json")
    assert control.count_output(root, 2) == 2
    assert control.count_output(root, 1) == 1
    assert control.count_output(tmp_path / "missing", 2) == 0


@pytest.mark.parametrize(
    "scenario,error",
    [
        ("success", None),
        ("no_checkpoint", "stateful checkpoint: timeout"),
        ("no_restore", "restore checkpoint: timeout"),
        ("short_output", "committed kept output .*timeout"),
        ("extra_output", "got 3, expected exactly 2"),
        ("cancel_stuck", "REST cancel confirmation: timeout"),
        ("cancel_extra", "after REST cancel: kept 3"),
    ],
)
def test_restart_control_protocol(monkeypatch, tmp_path, scenario, error):
    """Fake REST/Docker checks the order of recovery, checkpoint commit, cancel and per-step errors."""
    control = load_restart_control()
    oracle = tmp_path / "oracle.json"
    oracle.write_text(json.dumps(dict(epoch=2, dedup=[dict(kept=2)])))
    write_rows(tmp_path, "output/dedup/2/kept", [dict(epoch=2), dict(epoch=2)])
    evidence = tmp_path / "restart.json"
    calls = []
    state = dict(restarted=False, cancel=False, polls=0, clock=0.0)

    def fake_sleep(seconds):
        """Advance the fake clock so every timeout runs fast and deterministically."""
        state["clock"] += seconds

    def fake_run(command, **kwargs):
        """Record stop/start without calling real Docker."""
        calls.append(command[1])
        assert kwargs["check"] and kwargs["timeout"] == 0.5
        if command[1] == "start":
            state["restarted"] = True

    def fake_get(base, path):
        """Generate REST checkpoints before/after restore together with the cancel state."""
        if path == "/jobs/overview":
            return dict(
                jobs=[dict(name="order-lifecycle-stream-dedup-2", jid="abc", state="RUNNING")]
            )
        if path.endswith("/checkpoints"):
            if not state["restarted"]:
                return dict(
                    latest=dict(
                        completed=dict(id=4, state_size=0 if scenario == "no_checkpoint" else 99)
                    )
                )
            state["polls"] += 1
            if scenario == "no_restore" or state["polls"] == 1:
                return dict(latest={})
            completed_id = 4 if state["polls"] == 2 else 5
            return dict(
                latest=dict(completed=dict(id=completed_id, state_size=99), restored=dict(id=4))
            )
        return dict(
            state="CANCELED" if state["cancel"] and scenario != "cancel_stuck" else "RUNNING"
        )

    def fake_count(root, epoch):
        """Fake missing/extra output at the wait point or after cancel."""
        assert epoch == 2 and root == tmp_path / "output/dedup/2/kept"
        calls.append("count")
        if scenario == "extra_output" or (scenario == "cancel_extra" and state["cancel"]):
            return 3
        return 1 if scenario == "short_output" else 2

    def fake_cancel(base, job):
        """Cancel may only be sent after restore and a new completed checkpoint."""
        assert job == "abc" and state["polls"] >= 3
        calls.append("cancel")
        state["cancel"] = True

    monkeypatch.setattr(control.time, "monotonic", lambda: state["clock"])
    monkeypatch.setattr(control.time, "sleep", fake_sleep)
    monkeypatch.setattr(control.subprocess, "run", fake_run)
    monkeypatch.setattr(control, "get", fake_get)
    monkeypatch.setattr(control, "count_output", fake_count)
    monkeypatch.setattr(control, "cancel", fake_cancel)
    kwargs = dict(
        base="http://local",
        taskmanager="tm",
        evidence=evidence,
        oracle=oracle,
        output=tmp_path / "output",
        timeout=0.5,
    )
    if error:
        with pytest.raises((RuntimeError, TimeoutError), match=error):
            control.control(**kwargs)
        assert not evidence.exists()
    else:
        control.control(**kwargs)
        proof = json.loads(evidence.read_text())
        assert proof["restored_id"] == proof["before"]["latest"]["completed"]["id"] == 4
        assert proof["after"]["latest"]["completed"]["id"] == 5
        assert proof["final_kept_count"] == proof["expected_kept"] == 2
        assert proof["final_state"] == "CANCELED"
        assert proof["timings"]["total_seconds"] > 0
        assert calls == ["stop", "start", "count", "cancel", "count"]
