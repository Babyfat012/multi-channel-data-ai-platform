"""Push tables, jobs (Airflow DAGs, the Flink Bronze stream job) and lineage to DataHub.

Run once DataHub GMS is ready. Check results are not published here: each Airflow validate task reports its own
result per run (dags/platform_datahub.py), and the Bronze Data Contracts come from
scripts/data/publish_datahub_contracts.py. The historical manifests in docs/evidence/ stay as evidence only.
"""

from __future__ import annotations

import argparse
import os

CHANNELS = ("amazon", "walmart", "tiktok_shop", "shopify", "shopee")
BRONZE = {c: f"lakehouse.bronze.raw_{c}_orders_dp1" for c in CHANNELS}
SILVER = {c: f"lakehouse.silver.stg_{c}_order_lines_dp2" for c in CHANNELS}
FACT = "lakehouse.gold.fact_order_lines_dp2"
DIM = "lakehouse.gold.dim_amazon_order_dp2"
FEATURE = "lakehouse.gold.feat_order_line_return_dp3"
LABELS = "lakehouse.gold.fact_return_labels_dp3"
# The Walmart-only label table (old dp3_walmart_labels DAG) stays on DataHub as historical evidence;
# the dp3_return_labels DAG now writes one label table for all five Channels.
LABELS_LEGACY = "lakehouse.gold.fact_walmart_return_labels_dp3"
# Flink (lakehouse-bronze-streams) writes the v2 table (with source_epoch); dp_nightly gates on it. The v1 table
# stays on DataHub as historical evidence of the 2026-10-13 run; no job writes to it anymore.
CDC = "lakehouse.bronze.raw_walmart_cdc_v2"
CDC_LEGACY = "lakehouse.bronze.raw_walmart_cdc"
# DP4: 30 days of five-Channel Gold on EC2 PostgreSQL (warehouse serving per-SKU lookups, indexed).
WAREHOUSE = "warehouse.fact_order_lines_30d_dp2"
DATASETS = (
    *BRONZE.values(),
    *SILVER.values(),
    FACT,
    DIM,
    FEATURE,
    LABELS,
    LABELS_LEGACY,
    CDC,
    CDC_LEGACY,
    WAREHOUSE,
)
RAW_AMAZON = "/data/raw/source-exports/channel=amazon/dataset=orders"
RAW_FILES = {
    channel: f"/data/channels-small/source-exports/channel={channel}/dataset=orders"
    for channel in ("tiktok_shop", "shopify", "shopee")
}
RAW_WALMART = "walmart.orders"
KAFKA_WALMART_CDC = "platform_cdc.walmart.orders"
SOURCE_DATASETS = {
    RAW_AMAZON: "file",
    **{name: "file" for name in RAW_FILES.values()},
    RAW_WALMART: "postgres",
    KAFKA_WALMART_CDC: "kafka",
    WAREHOUSE: "postgres",
}


def raw_sources(raw_root: str | None) -> dict[str, str]:
    """File source paths per Channel; ``raw_root`` (e.g. s3a://bucket/.../source-exports) replaces the local set."""
    if raw_root is None:
        return {"amazon": RAW_AMAZON, **RAW_FILES}
    return {
        c: f"{raw_root.rstrip('/')}/channel={c}/dataset=orders"
        for c in ("amazon", "tiktok_shop", "shopify", "shopee")
    }


def job_map(
    raw_root: str | None = None, dags: tuple[str, ...] | None = None
) -> dict[str, dict[str, tuple[list[str], list[str]]]]:
    """Airflow task -> tables actually read/written per project DAG; keep only ``dags`` if given."""
    files = raw_sources(raw_root)
    dp1: dict[str, tuple[list[str], list[str]]] = {}
    dp2: dict[str, tuple[list[str], list[str]]] = {}
    for channel in CHANNELS:
        source = RAW_WALMART if channel == "walmart" else files[channel]
        dp1[f"ingest_{channel}"] = ([source], [BRONZE[channel]])
        dp1[f"validate_{channel}"] = ([BRONZE[channel]], [])
        dp2[f"ingest_{channel}_silver"] = ([BRONZE[channel]], [SILVER[channel]])
        dp2[f"validate_{channel}_silver"] = ([SILVER[channel]], [])
    dp2["build_gold_order_lines"] = (list(SILVER.values()), [FACT])
    dp2["build_amazon_order_dim"] = ([BRONZE["amazon"]], [DIM])
    dp2["validate_amazon_order_dim"] = ([DIM, FACT], [])
    dp2["validate_gold_order_lines"] = ([FACT], [])
    jobs = {
        "dp1_bronze": dp1,
        "lakehouse-bronze-streams": {"walmart_cdc_to_bronze": ([KAFKA_WALMART_CDC], [CDC])},
        "dp_nightly": {"check_walmart_cdc_complete": ([CDC], [])},
        "dp2_silver": dp2,
        "dp3_features": {
            "build_return_features": ([FACT], [FEATURE]),
            "validate_return_features": ([FEATURE], []),
        },
        "dp3_return_labels": {
            "build_and_validate_return_labels": ([FACT], [LABELS]),
        },
        "dp4_warehouse": {
            "export_and_index_warehouse": ([FACT], [WAREHOUSE]),
        },
    }
    return {dag: tasks for dag, tasks in jobs.items() if dags is None or dag in dags}


# Every flow is an Airflow DAG except the always-on Flink job.
FLOW_PLATFORM = {"lakehouse-bronze-streams": "flink"}


def datasets_of(jobs: dict[str, dict[str, tuple[list[str], list[str]]]]) -> list[str]:
    """Tables/sources the selected jobs read or write, in order of appearance."""
    names = (n for tasks in jobs.values() for io in tasks.values() for side in io for n in side)
    return list(dict.fromkeys(names))


def platform_of(name: str) -> str:
    """DataHub platform of a dataset based on its prefix/name."""
    if name.startswith(("s3a://", "s3://")):
        return "s3"
    return SOURCE_DATASETS.get(name, "iceberg")


def description(name: str, platform: str) -> str:
    """Dataset description; the CDC v1 table is explicitly marked as historical."""
    if name == CDC_LEGACY:
        return f"PLATFORM {platform}: {name} (legacy CDC v1, evidence of run 2026-10-13; DAG now writes {CDC})"
    if name == LABELS_LEGACY:
        return f"PLATFORM {platform}: {name} (legacy Walmart-only labels; DAG now writes {LABELS})"
    return f"PLATFORM {platform}: {name}"


def main() -> None:
    """Publish datasets, jobs and lineage to DataHub GMS."""
    parser = argparse.ArgumentParser(description="Publish PLATFORM datasets and lineage to DataHub")
    parser.add_argument("--server", default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"))
    parser.add_argument(
        "--raw-root",
        help="Root of the real file sources, e.g. s3a://<raw-bucket>/order_snapshots/source-exports (AWS)",
    )
    parser.add_argument(
        "--dags", nargs="+", choices=sorted(job_map()), help="Publish only DAGs that actually ran"
    )
    args = parser.parse_args()
    jobs = job_map(args.raw_root, tuple(args.dags) if args.dags else None)
    # No DAG selected: also keep historical tables no job writes anymore (CDC v1), as before.
    names = datasets_of(jobs) + ([] if args.dags else list(DATASETS))
    names = list(dict.fromkeys(names))

    # Import at run time so the file still loads when the client is not installed.
    from datahub.metadata.urns import DatasetUrn
    from datahub.sdk import DataFlow, DataHubClient, DataJob, Dataset

    token = os.getenv("DATAHUB_GMS_TOKEN")
    client = DataHubClient(server=args.server, token=token)

    def urn(name: str) -> DatasetUrn:
        """Resolve a dataset URN using its actual storage platform."""
        return DatasetUrn(platform=platform_of(name), name=name, env="PROD")

    for name in names:
        platform = platform_of(name)
        client.entities.upsert(
            Dataset(
                platform=platform,
                name=name,
                description=description(name, platform),
            )
        )

    for dag_id, tasks in jobs.items():
        platform = FLOW_PLATFORM.get(dag_id, "airflow")
        flow = DataFlow(
            platform=platform, name=dag_id, description=f"PLATFORM {platform} job {dag_id}"
        )
        client.entities.upsert(flow)
        for task_id, (inputs, outputs) in tasks.items():
            client.entities.upsert(
                DataJob(
                    name=task_id,
                    flow=flow,
                    inlets=[urn(x) for x in inputs],
                    outlets=[urn(x) for x in outputs],
                )
            )
            for source in inputs:
                for target in outputs:
                    client.lineage.add_lineage(upstream=urn(source), downstream=urn(target))

    print(f"DataHub: {len(names)} datasets, {sum(map(len, jobs.values()))} jobs")


if __name__ == "__main__":
    main()
