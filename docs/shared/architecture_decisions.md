# Data & AI Platform: deployed Data Platform architecture

The platform consolidates data from a Seller's five Channels into three layers: Bronze, Silver,
Gold. The [deployment diagram](../diagrams/data-platform-system-architecture-excalidraw.png) shows the containers and data
flows; the [terminology](terminology.md) defines the layers and business objects.

## Iceberg, MinIO and the REST catalog

Lakehouse tables use Iceberg with data stored on MinIO; Lakekeeper provides the
REST catalog and stores operational metadata in PostgreSQL. Spark handles Batch processing,
and Trino reads the tables through the catalog. Configuration and the measurements that were run are recorded in
[Spark](../data-platform/reference/spark.md), [Storage](../data-platform/data_storage.md) and [Trino](../data-platform/reference/trino.md).

Bronze keeps the source JSON payload and provenance. The two Amazon schemas, which differ in names
and structure, are handled explicitly in Silver with two parse schemas, column
normalization and then dedup; the table format's schema evolution does not perform
this payload mapping by itself. Amazon SCD2 takes the versions observed in Bronze
and does not reconstruct states that were never exported.

## The warehouse is a query-serving copy

Gold Iceberg is the primary output. A job exports a Walmart slice of Gold to the PostgreSQL
warehouse to measure indexing and to compare data between the two systems with Trino. The current
evidence covers 1,875,607 Walmart Order Lines; there is no claim that all of Gold for the five
Channels has been replicated. The index DDL, EXPLAIN and the same-SKU comparison are in
[Storage](../data-platform/data_storage.md) and [Extension mechanisms](../data-platform/reference/data_integration.md).

## Orchestration and governance

Airflow orchestrates DP1 sources → Bronze, DP2 Bronze → Silver/Gold/SCD2 and DP3
offline features, with Validate after each stage. Connections/Variables are stored
in Airflow to be shared across DAGs. The jobs currently write tables and then Validate;
there is no claim that a Write-Audit-Publish branch mechanism has been implemented.

DataHub receives lineage and assertions extracted from successful Validate logs. The
assertions are currently evidence for the recorded run; publishing after every run
is not automated yet. See [Airflow](../data-platform/pipeline_orchestration.md) and [DataHub](../data-platform/data_governance.md).

## Data scope and resources

The Batch manifest records 100,012,790,412 bytes for Amazon, Walmart and SKUs. The
DP1/DP2/DP3 runs use 10 days of Amazon, the existing Walmart tables and a small source set for the other three
Channels. Flink has proof of baseline, late arrival, dedup and windows on the
Stream source, recorded in [Flink](../data-platform/reference/flink.md).

Services are started in groups to fit the RAM and disk of a development machine. Do not
start the whole Compose stack when you only need to rerun one proof; the commands naming the exact
services are in each component's docs. Status and evidence are
summarized in the [Data Platform status](../data-platform/reference/data_platform_order_lifecycle_status.md) and [evidence](../data-platform/evidence.md).
