"""Tests for resuming after a Batch is interrupted midway.

The 100 GB run takes about 3.2 hours on one core. During that time the machine may have to
shut down. These tests pin exactly one promise: **interrupting and resuming gives a result
identical to an uninterrupted run**, down to every byte and every number in the manifest.

The interruption is simulated by raising from `write_json_atomic` — the only place that writes stamps.
The two kinds of interruption differ in nature, so both are tested:

- **Clean cut**: the stamp of day d was fully written before dying. Everything on disk is consistent.
- **Dirty cut**: the files of day d are on disk but the stamp was NOT yet written. This is the
  dangerous state: if the next run trusts those files, the data may be missing half its rows.
  Exactly the silent kind of breakage — no error, no red test, just wrong numbers.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from generator.generate_order_snapshots import BatchRun


class _Cut(Exception):
    """The machine shut down. Not a bug in the code."""


def _run_with_cut(config, cut_at: int, *, stamp_first: bool) -> None:
    """Run the Batch, then cut it at day `cut_at`.

    `stamp_first=True` is a clean cut (dies after the stamp is written), `False` is a dirty cut
    (dies right before stamping, leaving files without a stamp).
    """
    run = BatchRun(config)
    write_json_atomic = run.store.write_json_atomic

    def guarded(relative: str, obj):
        """Intercept the stamp write of the cut day: raise BEFORE writing (dirty cut) or AFTER writing (clean cut)."""
        is_stamp_of_cut_day = relative == f"_checkpoint/day-{cut_at:05d}.json"
        if is_stamp_of_cut_day and not stamp_first:
            raise _Cut
        written = write_json_atomic(relative, obj)
        if is_stamp_of_cut_day:
            raise _Cut
        return written

    run.store.write_json_atomic = guarded
    try:
        run.run()
    except _Cut:
        return
    raise AssertionError(f"could not cut at day {cut_at}")


def _tree(root: Path) -> dict[str, bytes]:
    """Every file under `root`, relative path -> byte content.

    `run_manifest.json` is excluded because it holds `storage.output_root`, i.e. the path of
    that run itself. It is compared separately via `_comparable`.
    """
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.name != "run_manifest.json"
    }


def _comparable(manifest: dict) -> dict:
    """The manifest without `output_root`, the only thing that must legitimately differ between the two directories."""
    stripped = json.loads(json.dumps(manifest))
    stripped["config"]["storage"].pop("output_root", None)
    return stripped


def _config_at(config, root: Path):
    """The same config but writing to a different `root`."""
    return dataclasses.replace(
        config, storage=dataclasses.replace(config.storage, output_root=str(root))
    )


def _straight_run(config, root: Path) -> dict:
    """Run uninterrupted into `root` and return the manifest."""
    return BatchRun(_config_at(config, root)).run()


@pytest.mark.parametrize("cut", [False, True])
@pytest.mark.parametrize("changed", ["seed", "days_history", "orders_per_day"])
def test_resume_rejects_changed_dataset_before_mutation(tiny_config, tmp_path, cut, changed):
    """A resume must not relabel old Orders with a new catalog/config, even after a crash."""
    config = dataclasses.replace(
        _config_at(tiny_config, tmp_path / "guarded"),
        scale=dataclasses.replace(tiny_config.scale, days_history=2, orders_per_day=10),
    )
    if cut:
        _run_with_cut(config, cut_at=1, stamp_first=False)
    else:
        BatchRun(config).run()
    root = Path(config.storage.output_root)
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    modified = (
        dataclasses.replace(config, seed=config.seed + 1)
        if changed == "seed"
        else dataclasses.replace(
            config,
            scale=dataclasses.replace(
                config.scale, **{changed: getattr(config.scale, changed) + 1}
            ),
        )
    )
    with pytest.raises(ValueError, match="identity|dataset"):
        BatchRun(modified).run()
    after = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert after == before, "Reject mismatches before rewriting SKU/manifest or discarding data"


@pytest.mark.parametrize("damaged", ["legacy", "checkpoint", "manifest", "runtime_identity"])
def test_untrusted_resume_metadata_is_rejected_without_repairing_it(tiny_config, tmp_path, damaged):
    """Never silently adopt legacy data or relabel a foreign checkpoint as this world."""
    config = dataclasses.replace(
        _config_at(tiny_config, tmp_path / "guarded"),
        scale=dataclasses.replace(tiny_config.scale, days_history=2, orders_per_day=10),
    )
    BatchRun(config).run()
    root = Path(config.storage.output_root)
    if damaged == "legacy":
        (root / "_dataset_identity.json").unlink()
    else:
        name = {
            "checkpoint": "_checkpoint/day-00001.json",
            "manifest": "run_manifest.json",
            "runtime_identity": "_dataset_identity.json",
        }[damaged]
        path = root / name
        value = json.loads(path.read_text())
        if damaged == "runtime_identity":
            value["runtime"]["numpy"] = "foreign-runtime"
        else:
            value["dataset_version"] = "foreign-world"
        path.write_text(json.dumps(value))
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="identity"):
        BatchRun(config).run()
    assert {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    } == before


def test_resuming_after_a_clean_cut_matches_a_straight_run_byte_for_byte(tiny_config, tmp_path):
    """Dies after the stamp is written: resume from the next day and get the same result."""
    straight = tmp_path / "straight"
    cut = tmp_path / "cut"
    expected = _straight_run(tiny_config, straight)

    config = _config_at(tiny_config, cut)
    _run_with_cut(config, cut_at=5, stamp_first=True)
    assert not (cut / "run_manifest.json").exists(), (
        "an interrupted run must not write the manifest"
    )

    resumed = BatchRun(config).run()
    assert _comparable(resumed) == _comparable(expected)
    assert _tree(cut) == _tree(straight)


def test_resuming_after_a_dirty_cut_matches_a_straight_run_byte_for_byte(tiny_config, tmp_path):
    """Dies BEFORE stamping: that day's files are discarded and regenerated, with no duplicates and nothing missing.

    This is the most important test of the mechanism. If `_discard_days_after` does not delete the files
    of a day without a stamp, the resumed run writes next to them and the data
    ends up duplicated — the manifest still adds up nicely, so only a byte comparison reveals it.
    """
    straight = tmp_path / "straight"
    cut = tmp_path / "cut"
    expected = _straight_run(tiny_config, straight)

    config = _config_at(tiny_config, cut)
    _run_with_cut(config, cut_at=5, stamp_first=False)
    # Day 5 left files on disk, but no stamp. Exactly the state that needs cleanup.
    assert sorted(cut.glob("**/part-sim00005.*")), "a dirty cut must leave day 5 files behind"
    assert not (cut / "_checkpoint/day-00005.json").exists()

    resumed = BatchRun(config).run()
    assert _comparable(resumed) == _comparable(expected)
    assert _tree(cut) == _tree(straight)


def test_the_run_picks_up_at_the_day_after_the_last_stamp(tiny_config, tmp_path):
    """A dirty cut at day 5 makes 4 the last complete day, and day 5 is regenerated."""
    config = _config_at(tiny_config, tmp_path / "cut")
    _run_with_cut(config, cut_at=5, stamp_first=False)

    run = BatchRun(config)
    assert run._last_complete_day() == 4
    removed = run._discard_days_after(4)
    assert removed, "day 5 files must be deleted"
    assert all("sim00005" in path or "day-00005" in path for path in removed), removed


def test_a_stamp_standing_alone_after_a_gap_is_not_trusted(tiny_config, tmp_path):
    """Stamps 1, 2 then a jump to 9: the last complete day is 2, not 9.

    A stray stamp does not prove the days before it are done. Taking the largest one found
    would skip days 3..8 and silently lose data.
    """
    root = tmp_path / "gap"
    config = _config_at(tiny_config, root)
    run = BatchRun(config)
    for day in (1, 2, 9):
        run.store.write_json_atomic(f"_checkpoint/day-{day:05d}.json", {})

    assert BatchRun(config)._last_complete_day() == 2


def test_the_stamp_is_written_only_after_every_file_of_the_day_is_closed(tiny_config):
    """The stamp is the last step of a day: when it is written, no file is still open.

    If the stamp is written before the files are closed, a power loss can leave a stamp saying "day d
    done" while day d's files are missing their tail. The next run trusts the stamp, skips day d, and
    data goes missing without anyone noticing. The whole mechanism lives or dies by this order.
    """
    run = BatchRun(tiny_config)
    write_json_atomic = run.store.write_json_atomic
    open_files_when_stamping = []

    def guarded(relative: str, obj):
        """Record the number of open files at the moment the stamp is written, to check the stamp is the day's last step."""
        if relative.startswith("_checkpoint/"):
            open_files_when_stamping.append(len(run.jsonl.keys()))
        return write_json_atomic(relative, obj)

    run.store.write_json_atomic = guarded
    run.run()

    assert len(open_files_when_stamping) == tiny_config.scale.days_history
    assert set(open_files_when_stamping) == {0}


def test_restart_ignores_the_stamps_and_regenerates_everything(tiny_config, tmp_path):
    """`--restart` ignores old stamps, starts over from day 1, and leaves no files of the old run."""
    straight = tmp_path / "straight"
    again = tmp_path / "again"
    expected = _straight_run(tiny_config, straight)

    config = _config_at(tiny_config, again)
    _run_with_cut(config, cut_at=7, stamp_first=True)
    assert (again / "_checkpoint/day-00007.json").exists()

    restarted = BatchRun(config).run(restart=True)
    assert _comparable(restarted) == _comparable(expected)
    assert _tree(again) == _tree(straight)


def test_resuming_a_finished_run_changes_nothing(tiny_config, tmp_path):
    """Rerunning on a finished directory regenerates no day and the result is unchanged."""
    root = tmp_path / "done"
    config = _config_at(tiny_config, root)
    first = BatchRun(config).run()
    before = _tree(root)

    run = BatchRun(config)
    assert run._last_complete_day() == tiny_config.scale.days_history
    assert run.run() == first
    assert _tree(root) == before


def test_the_manifest_of_a_resumed_run_still_matches_the_files_on_disk(tiny_config, tmp_path):
    """Byte and file counts in the manifest after resuming are still the actual numbers on disk.

    `datasets[*].bytes` is the evidence that the dataset reaches 100 GB. It accumulates
    through the stamps, so if `_counters` misses a field it is wrong exactly on the interrupted run
    — i.e. exactly on the real run, not in the uninterrupted test.
    """
    root = tmp_path / "resumed"
    config = _config_at(tiny_config, root)
    _run_with_cut(config, cut_at=6, stamp_first=False)
    manifest = BatchRun(config).run()

    patterns = {
        "amazon.orders": "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl",
        "walmart.orders": "pg-load/walmart.orders/export_date=*/part-sim*.jsonl",
        "seller.skus": "pg-load/seller.skus/part-00000.jsonl",
    }
    for dataset, pattern in patterns.items():
        parts = sorted(root.glob(pattern))
        assert parts, dataset
        assert manifest["datasets"][dataset]["bytes"] == sum(p.stat().st_size for p in parts), (
            dataset
        )
        assert manifest["datasets"][dataset]["files"] == len(parts), dataset

    rows = sum(len(p.read_text().splitlines()) for p in root.glob(patterns["amazon.orders"]))
    assert manifest["datasets"]["amazon.orders"]["rows"] == rows
    assert json.loads((root / "run_manifest.json").read_text()) == manifest


def test_a_shorter_rerun_removes_the_files_left_by_a_longer_previous_run(tiny_config, tmp_path):
    """Rerunning with fewer days must remove the files of days that no longer exist.

    This is why `_discard_days_after` exists. Rewriting a day overwrites its old files
    (opening with `"wb"` truncates), so the cut day needs no cleanup. But the files of
    days 13..20 left by the previous run are overwritten by NO day — they just stay there.

    Consequence without cleanup: Spark, DuckDB and `generator/report.py` read the whole directory, so
    they still see them and count them as real data, while the manifest does not. Then
    `datasets[*].bytes` — the evidence of reaching 100 GB — no longer equals the actual bytes
    on disk, and nothing reports an error.
    """
    root = tmp_path / "shrink"
    fresh = tmp_path / "fresh"
    longer = dataclasses.replace(tiny_config.scale, days_history=20)
    BatchRun(_config_at(dataclasses.replace(tiny_config, scale=longer), root)).run()
    assert sorted(root.glob("**/part-sim00018.*")), "a 20-day run must have day 18 files"

    expected = _straight_run(tiny_config, fresh)
    shrunk = BatchRun(_config_at(tiny_config, root)).run(restart=True)

    assert _comparable(shrunk) == _comparable(expected)
    assert _tree(root) == _tree(fresh)

    for dataset, pattern in (
        (
            "amazon.orders",
            "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl",
        ),
        ("walmart.orders", "pg-load/walmart.orders/export_date=*/part-sim*.jsonl"),
    ):
        on_disk = sum(p.stat().st_size for p in root.glob(pattern))
        assert shrunk["datasets"][dataset]["bytes"] == on_disk, dataset
