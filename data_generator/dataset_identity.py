"""Fingerprint the world/config, generator source and runtime used by a batch.

The output location is transport metadata: copying an intact dataset must not
change its identity. Mapping iteration order is kept explicitly because the
simulator samples Storefronts in configuration order.
"""

from __future__ import annotations

import hashlib
import json
import platform
from pathlib import Path
from typing import Any

import fsspec
import numpy as np
import pyarrow as pa

from data_generator.config import GeneratorConfig


def build_dataset_identity(config: GeneratorConfig) -> dict[str, Any]:
    """Return a deterministic fingerprint, excluding only the output location."""
    configuration = config.to_dict()
    configuration["storage"].pop("output_root", None)
    root = Path(__file__).parent
    source_hash = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        source_hash.update(path.relative_to(root).as_posix().encode("utf-8"))
        source_hash.update(b"\0")
        source_hash.update(path.read_bytes())
        source_hash.update(b"\0")
    identity = {
        "format_version": 1,
        "configuration": configuration,
        "storefront_order": list(config.storefront_weights),
        "generator_sha256": source_hash.hexdigest(),
        "runtime": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pyarrow": pa.__version__,
            "fsspec": fsspec.__version__,
        },
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {**identity, "dataset_version": hashlib.sha256(canonical.encode("utf-8")).hexdigest()}
