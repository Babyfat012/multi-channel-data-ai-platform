"""Registry in Git: loads schemas and checks rules, reusing the parser."""

import json
import os
from pathlib import Path

from spark_jobs.lib.data_contracts.compat import diff, verdict

DOCKER_CONTRACTS_DIR = Path("/opt/contracts")
REPO_CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def resolve_contracts_dir(contracts_dir=None):
    """Prefer the flag, then env, then Docker if it exists, then contracts next to the package root."""
    if contracts_dir is not None:
        return Path(contracts_dir)
    environment = os.environ.get("PLATFORM_CONTRACTS_DIR")
    if environment is not None:
        return Path(environment)
    return DOCKER_CONTRACTS_DIR if DOCKER_CONTRACTS_DIR.is_dir() else REPO_CONTRACTS_DIR


NATIVE_PARSERS = {"amazon/1", "amazon/2", "walmart/1", "shopify/1", "tiktok_shop/1", "shopee/1"}
OPERATIONAL_FEEDS = (
    "seller_oms.line_commitments",
    "seller_oms.fulfillment_allocations",
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
    "seller_catalog.listing_versions",
    "seller_wms.stock_operations",
    "seller_wms.inbound_events",
    "seller_wms.stock_snapshots",
)
SIDE_FEEDS = ("source.coverage_envelopes", "source.uncertainties")
SUPPORTED_PARSERS = NATIVE_PARSERS | set(OPERATIONAL_FEEDS) | set(SIDE_FEEDS)


class Registry:
    """schema_id index for the driver and CLI, no network loading."""

    def __init__(self, contracts_dir=None):
        """Read one registry snapshot and the schemas relative to the contracts directory."""
        self.root = resolve_contracts_dir(contracts_dir)
        self.document = json.loads((self.root / "registry.json").read_text())
        self.entries = {}
        for subject in self.document["subjects"].values():
            for version in subject["versions"]:
                entry = dict(version)
                entry["schema"] = json.loads((self.root / version["schema_path"]).read_text())
                if entry["schema_id"] in self.entries:
                    raise ValueError(f"Duplicate schema_id: {entry['schema_id']}")
                self.entries[entry["schema_id"]] = entry

    def check_registry(self):
        """List breaking versions that still reuse the previous parser, with the changed paths."""
        errors = []
        for name, subject in self.document["subjects"].items():
            versions = subject["versions"]
            for version in versions:
                if "identity_path" in version and (
                    not isinstance(version["identity_path"], str)
                    or not version["identity_path"].startswith("$.")
                ):
                    errors.append(
                        f"{version['schema_id']}: identity_path must be a string starting with $."
                    )
            for old, new in zip(versions, versions[1:]):
                changes = diff(
                    self.entries.get(old["schema_id"])["schema"],
                    self.entries.get(new["schema_id"])["schema"],
                    new.get("migrations"),
                )
                if (
                    not verdict(changes, subject["compatibility"])
                    and new["parser"] is not None
                    and new["parser"] == old["parser"]
                ):
                    errors.append(
                        f"{name}: {new['schema_id']} breaking change reuses parser {new['parser']}: "
                        + ", ".join(
                            f"{c.kind} {c.path}" + (f" -> {c.target}" if c.target else "")
                            for c in changes
                        )
                    )
        return errors
