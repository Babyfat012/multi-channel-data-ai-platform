"""Keep the Redis recovery contract: AOF lives on a PVC that is reused when the Pod is replaced."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_redis_aof_uses_persistent_volume_without_overlapping_rollout():
    """Block AOF being disabled/written outside the PVC, a wrong claim, and a rollout overlapping two Redis instances."""
    documents = list(yaml.safe_load_all((ROOT / "infra/k8s/feature-store/redis.yaml").read_text()))
    deployment = next(d for d in documents if d["kind"] == "Deployment")
    spec = deployment["spec"]
    assert spec["replicas"] == 1
    assert spec["strategy"]["type"] == "Recreate"
    pod = spec["template"]["spec"]
    redis = next(c for c in pod["containers"] if c["name"] == "redis")
    args = redis["args"]
    options = dict(zip(args[::2], args[1::2], strict=True))
    assert options["--appendonly"] == "yes"
    assert options["--appendfsync"] == "everysec"
    assert options["--maxmemory-policy"] == "noeviction"
    mount = next(m for m in redis["volumeMounts"] if m["mountPath"] == options["--dir"])
    assert not mount.get("readOnly", False)
    volume = next(v for v in pod["volumes"] if v["name"] == mount["name"])
    claim = next(
        d
        for d in documents
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == volume["persistentVolumeClaim"]["claimName"]
    )
    assert claim["metadata"]["namespace"] == deployment["metadata"]["namespace"]
    assert claim["spec"]["accessModes"] == ["ReadWriteOnce"]
    assert claim["spec"]["resources"]["requests"]["storage"] == "2Gi"
