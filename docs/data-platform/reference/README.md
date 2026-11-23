# Technical reference

Design details, development guides and individually scoped runs. AWS acceptance results are in the [documentation index](../README.md); local results do not replace cloud evidence.

- [Debezium CDC for the Walmart source](cdc.md)
- [Sources and confidence level of each Channel's data shape](channel_sources.md)
- [Data contracts — data contract, schema registry and Silver quarantine gate](data_contracts.md)
- [Federation and CDC — comparison method](data_integration.md)
- [Data Platform](data_platform.md)
- [Data Generator data quality report (Batch)](data_quality_report_batch.md)
- [Data Generator data quality report (Stream)](data_quality_report_stream.md)
- [Flink on the generator's two Kafka topics](flink.md)
- [Data Generator design](generator_design.md)
- [Gold Order Line and offline features](gold.md)
- [Gold layer design — implemented decisions](gold_design.md)
- [Iceberg maintenance — policy-driven Iceberg self-maintenance](iceberg_maintenance.md)
- [Silver layer design — cleaning and mapping to a shared vocabulary](silver.md)
- [Silver for the three new Batch Channels](silver_five_channels.md)
- [Spark optimization — Bronze stage](spark.md)
- [Trino: querying Iceberg and PostgreSQL in one SQL session](trino.md)
