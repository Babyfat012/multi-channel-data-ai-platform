"""Check Iceberg S3 write/read with a dedicated temporary table, then drop exactly that table."""

import sys
import uuid

sys.path.insert(0, "/opt")
from spark_jobs.lib.iceberg_session import build_spark


def main():
    """Reconcile data and S3 location; drop the table in finally, even on failure."""
    spark = build_spark("s3-runtime-smoke", shuffle_partitions=2)
    table = "lakehouse.runtime_smoke.probe_" + uuid.uuid4().hex
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.runtime_smoke")
        spark.range(3).writeTo(table).create()
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [0, 1, 2]
        locations = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        assert any(r.col_name == "Location" and r.data_type.startswith("s3://") for r in locations)
        print("S3 Iceberg smoke: PASS")
    finally:
        # No PURGE: Spark drops the table from the catalog before re-reading metadata to delete files,
        # by which point Lakekeeper no longer has the table and refuses. Lakekeeper cleans up files of dropped tables itself.
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.stop()


if __name__ == "__main__":
    main()
