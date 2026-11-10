"""Test data contracts through the public Python interface."""

from spark_jobs.lib.data_contracts.schema import infer_schema, required_json_paths, validate_record


def test_infer_and_validate():
    """Distinguish common fields, optional fields and nested type unions."""
    schema = infer_schema(
        [{"a": 1, "items": [{"id": "x"}], "note": "hi"}, {"a": None, "items": [{"id": "y"}]}]
    )
    assert schema["required"] == ["a", "items"]
    assert schema["properties"]["a"]["type"] == ["integer", "null"]
    assert validate_record({"a": None, "items": [{"id": "ok"}]}, schema) == []
    assert "$.items[0].id" in required_json_paths(schema)
    assert validate_record({"a": "wrong", "items": [{}]}, schema)


def test_compatibility_rename_and_optional():
    """An explained rename still breaks; adding an optional field is compatible."""
    from spark_jobs.lib.data_contracts.compat import diff, verdict

    old = infer_schema([{"order_sn": "a", "items": [{"model_sku": "x"}]}])
    new = infer_schema([{"order_number": "a", "items": [{"sku": "x"}]}])
    changes = diff(old, new)
    assert {c.path for c in changes if c.kind == "field_removed"} == {
        "$.order_sn",
        "$.items[0].model_sku",
    }
    assert not verdict(changes, "BACKWARD")
    assert not verdict(changes, "FORWARD")
    renamed = diff(
        old, new, {"$.order_sn": "$.order_number", "$.items[0].model_sku": "$.items[0].sku"}
    )
    assert len([c for c in renamed if c.kind == "field_renamed"]) == 2
    assert not verdict(renamed, "BACKWARD")
    assert not verdict(renamed, "FORWARD")
    optional = infer_schema(
        [
            {"order_sn": "a", "items": [{"model_sku": "x"}], "message": "hello"},
            {"order_sn": "b", "items": [{"model_sku": "y"}]},
        ]
    )
    assert verdict(diff(old, optional), "BACKWARD")


def test_generator_overrides_manifest(tmp_path):
    """The override declares the right schema for each Channel in the manifest."""
    from generator.generate_order_lifecycle_fixture import generate_fixture

    manifest = generate_fixture(
        tmp_path / "override", native_schemas={"shopee": 2, "tiktok_shop": 2}
    )
    assert manifest["native_sources"]["shopee"]["schema_id"] == "sim-native-v2/shopee/2"
    assert manifest["native_sources"]["amazon"]["schema_id"] == "sim-native-v2/amazon/1"


def test_registry_and_gate(tmp_path):
    """Three fixtures check the gate and the parser rule when registering a breaking schema."""
    import json
    import shutil

    from generator.generate_order_lifecycle_fixture import generate_fixture
    from spark_jobs.lib.data_contracts.gate import gate_root
    from spark_jobs.lib.data_contracts.registry import Registry

    registry = Registry("contracts")
    assert registry.check_registry() == []
    for name, overrides, expected in [
        ("default", {}, 0),
        ("shopee", {"shopee": 2}, 3),
        ("tiktok", {"tiktok_shop": 2}, 0),
    ]:
        root = tmp_path / name
        manifest = generate_fixture(root, native_schemas=overrides)
        report = gate_root(root, registry)
        assert report["quarantine"]["total"] == expected
        if expected:
            assert report["quarantine"]["by_reason"] == {"unregistered_schema": 3}
            assert all(
                f["accepted"] == f["rows"] for f in report["files"] if f["feed"] != "shopee.orders"
            )
            shopee_file = next(f for f in manifest["files"] if f["feed"] == "shopee.orders")
            schema = infer_schema(
                json.loads(line) for line in (root / shopee_file["path"]).read_text().splitlines()
            )
    target = tmp_path / "contracts"
    shutil.copytree("contracts", target)
    path = target / "schemas/sim-native-v2/shopee/2.schema.json"
    path.write_text(json.dumps(schema))
    index = json.loads((target / "registry.json").read_text())
    index["subjects"]["sim-native-v2/shopee"]["versions"].append(
        {
            "schema_id": "sim-native-v2/shopee/2",
            "schema_path": "schemas/sim-native-v2/shopee/2.schema.json",
            "parser": "shopee/1",
            "status": "active",
        }
    )
    (target / "registry.json").write_text(json.dumps(index))
    errors = Registry(target).check_registry()
    assert errors
    assert all(field in str(errors) for field in ("order_sn", "order_number", "model_sku", "sku"))


def test_schema_validates_all_items_and_json_types():
    """Boolean is not integer; the validator also catches the second element."""
    schema = {
        "type": "object",
        "required": ["n", "rows"],
        "properties": {
            "n": {"type": ["number", "null"]},
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["id"],
                    "properties": {"id": {"type": "integer"}},
                },
            },
        },
    }
    assert validate_record({"n": 1, "rows": []}, schema) == []
    errors = validate_record({"n": True, "rows": [{"id": 1}, {}]}, schema)
    assert any("$.n: type boolean" in error for error in errors)
    assert "$.rows[1].id: missing required" in errors
    assert "$.rows: missing required" in validate_record({"n": None}, schema)


def test_compatibility_modes_required_transitions_and_types():
    """Check all four modes, type changes and both directions of required changes."""
    from spark_jobs.lib.data_contracts.compat import diff, verdict

    old = {"type": "object", "properties": {"a": {"type": "string"}}, "required": []}
    required = {**old, "required": ["a"]}
    assert [c.kind for c in diff(old, required)] == ["became_required"]
    assert not verdict(diff(old, required), "BACKWARD")
    assert verdict(diff(old, required), "FORWARD")
    assert verdict(diff(required, old), "BACKWARD")
    assert not verdict(diff(required, old), "FORWARD")
    changed = infer_schema([{"a": 1}])
    for mode in ("BACKWARD", "FORWARD", "FULL"):
        assert not verdict(diff(required, changed), mode)
    assert verdict(diff(required, changed), "NONE")


def test_gate_reason_precedence_and_full_validation(tmp_path):
    """Unknown label before parser; null parser before validation; only a known parser checks the contract."""
    import json

    from generator.generate_order_lifecycle_fixture import generate_fixture
    from spark_jobs.lib.data_contracts.gate import gate_root
    from spark_jobs.lib.data_contracts.registry import Registry

    root = tmp_path / "source"
    manifest = generate_fixture(root)
    source = next(f for f in manifest["files"] if f["feed"] == "shopee.orders")
    path = root / source["path"]
    records = [json.loads(line) for line in path.read_text().splitlines()]
    records[0]["schema_id"] = "unknown/99"
    records[1]["response"]["order_list"][0]["item_list"][1].pop("model_sku")
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    registry = Registry("contracts")
    report = gate_root(root, registry)
    assert report["quarantine"]["by_reason"] == {"unregistered_schema": 1, "contract_violation": 1}
    assert "model_sku" in str(report["files"])
    registry.entries["sim-native-v2/shopee/1"]["parser"] = None
    report = gate_root(root, registry)
    assert report["quarantine"]["by_reason"] == {"unregistered_schema": 1, "no_parser": 2}


def test_cli_rejects_registry_with_reused_breaking_parser(tmp_path):
    """check-registry exits non-zero and points to the renamed field."""
    import json
    import shutil
    import subprocess
    import sys

    target = tmp_path / "contracts"
    shutil.copytree("contracts", target)
    path = target / "schemas/sim-native-v2/shopee/2.schema.json"
    schema = json.loads((target / "schemas/sim-native-v2/shopee/1.schema.json").read_text())
    order = schema["properties"]["response"]["properties"]["order_list"]["items"]
    order["properties"]["order_number"] = order["properties"].pop("order_sn")
    order["required"].remove("order_sn")
    order["required"].append("order_number")
    path.write_text(json.dumps(schema))
    index = json.loads((target / "registry.json").read_text())
    index["subjects"]["sim-native-v2/shopee"]["versions"].append(
        {
            "schema_id": "sim-native-v2/shopee/2",
            "schema_path": "schemas/sim-native-v2/shopee/2.schema.json",
            "parser": "shopee/1",
            "status": "active",
        }
    )
    (target / "registry.json").write_text(json.dumps(index))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "spark_jobs.lib.data_contracts",
            "check-registry",
            "--contracts-dir",
            str(target),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "order_sn" in result.stdout and "order_number" in result.stdout


def test_registry_identity_paths(tmp_path):
    """Native identity lives in the registry; bad metadata is rejected by check-registry."""
    import json
    import shutil

    from spark_jobs.lib.data_contracts.registry import Registry

    registry = Registry("contracts")
    native = [entry for sid, entry in registry.entries.items() if sid.startswith("sim-native-v2/")]
    assert len(native) == 7  # Six original schemas plus TikTok /2 sharing the same identity.
    assert all(entry["identity_path"].startswith("$.") for entry in native)
    target = tmp_path / "contracts"
    shutil.copytree("contracts", target)
    index = json.loads((target / "registry.json").read_text())
    version = index["subjects"]["sim-native-v2/shopee"]["versions"][0]
    for invalid in (None, 123, "order_sn", "$"):
        version["identity_path"] = invalid
        (target / "registry.json").write_text(json.dumps(index))
        errors = Registry(target).check_registry()
        assert any(
            "identity_path" in error and "sim-native-v2/shopee/1" in error for error in errors
        )
    version.pop("identity_path")
    (target / "registry.json").write_text(json.dumps(index))
    assert Registry(target).check_registry() == []


def test_contracts_directory_precedence(tmp_path, monkeypatch, capsys):
    """Flag beats env, env beats Docker, an existing Docker path beats the repo, independent of cwd."""
    import shutil
    from pathlib import Path

    from spark_jobs.lib.data_contracts import registry as module
    from spark_jobs.lib.data_contracts.__main__ import main

    repo = Path(module.__file__).resolve().parents[3] / "contracts"
    explicit, environment, docker = [tmp_path / name for name in ("flag", "env", "docker")]
    for target in (explicit, environment, docker):
        shutil.copytree(repo, target)
    monkeypatch.setattr(module, "DOCKER_CONTRACTS_DIR", docker)
    monkeypatch.setenv("PLATFORM_CONTRACTS_DIR", str(environment))
    monkeypatch.chdir(tmp_path)
    assert module.Registry(explicit).root == explicit
    assert module.Registry().root == environment
    assert main(["check-registry", "--contracts-dir", str(explicit)]) == 0
    assert main(["check-registry"]) == 0
    monkeypatch.delenv("PLATFORM_CONTRACTS_DIR")
    assert module.Registry().root == docker
    shutil.rmtree(docker)
    assert module.Registry().root == repo
    assert main(["check-registry"]) == 0
    assert '"errors": []' in capsys.readouterr().out


def test_cli_gate_uses_contracts_directory_precedence(tmp_path, monkeypatch, capsys):
    """The gate command uses the same resolver as check-registry and Registry directly."""
    import json
    import shutil
    from pathlib import Path

    from generator.generate_order_lifecycle_fixture import generate_fixture
    from spark_jobs.lib.data_contracts import registry as module
    from spark_jobs.lib.data_contracts.__main__ import main

    fixture = tmp_path / "source"
    generate_fixture(fixture)
    target = tmp_path / "contracts"
    shutil.copytree(Path(module.__file__).resolve().parents[3] / "contracts", target)
    monkeypatch.setenv("PLATFORM_CONTRACTS_DIR", str(tmp_path / "missing"))
    assert main(["gate", str(fixture), "--contracts-dir", str(target)]) == 0
    assert json.loads(capsys.readouterr().out)["quarantine"]["total"] == 0
    monkeypatch.setenv("PLATFORM_CONTRACTS_DIR", str(target))
    assert main(["gate", str(fixture)]) == 0
    assert json.loads(capsys.readouterr().out)["quarantine"]["total"] == 0
