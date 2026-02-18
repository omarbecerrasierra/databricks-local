"""
Medallion ETL demo — runs inside the Docker Databricks emulator.

    docker compose exec spark python main.py
"""

import os

from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    upper,
    round as spark_round,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from delta.tables import DeltaTable

from databricks_shim import get_spark_session


# ── Schema & sample data ─────────────────────────────────────────────────
SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", DoubleType()),
        StructField("category", StringType()),
    ]
)

PRODUCTS = [
    (1, "Product A", 100.0, "electronics"),
    (2, "Product B", 200.0, "electronics"),
    (3, "Product C", 50.0, "clothing"),
    (4, "Product D", 75.0, "clothing"),
    (5, "Product E", 300.0, "home"),
]


def paths(bucket, prefix):
    """Return bronze / silver / gold paths."""
    return (
        f"{prefix}://{bucket}/bronze/products",
        f"{prefix}://{bucket}/silver/products",
        f"{prefix}://{bucket}/gold/category_summary",
    )


# ── ETL steps ─────────────────────────────────────────────────────────────
def ingest_bronze(spark, bronze):
    print("Bronze  ▸ raw ingestion")
    spark.createDataFrame(PRODUCTS, SCHEMA).write.format("delta").mode(
        "overwrite"
    ).save(bronze)


def process_silver(spark, bronze, silver):
    print("Silver  ▸ cleansed + enriched")
    df = (
        spark.read.format("delta")
        .load(bronze)
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("name_upper", upper(col("name")))
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS sales")
    # Write to path first, then create table pointing to it
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        silver
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS sales.products_silver USING DELTA LOCATION '{silver}'"
    )


def aggregate_gold(spark, silver, gold):
    print("Gold    ▸ business aggregates")
    df = (
        spark.read.format("delta")
        .load(silver)
        .groupBy("category")
        .agg(avg("price").alias("avg_price"), count("id").alias("product_count"))
        .withColumn("avg_price", spark_round(col("avg_price"), 2))
    )
    # Write to path first, then create table pointing to it
    df.write.format("delta").mode("overwrite").save(gold)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS sales.category_summary_gold USING DELTA LOCATION '{gold}'"
    )


def simulate_append(spark, bronze):
    print("Append  ▸ new rows to Bronze (time-travel demo)")
    new = [(6, "Product F", 150.0, "home"), (7, "Product G", 400.0, "electronics")]
    spark.createDataFrame(new, SCHEMA).write.format("delta").mode("append").save(bronze)


def merge_into_silver(spark, silver):
    print("Merge   ▸ upsert into Silver")
    updates = [
        (1, "Product A Premium", 120.0, "electronics"),
        (8, "Product H", 90.0, "clothing"),
    ]
    src = (
        spark.createDataFrame(updates, SCHEMA)
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("name_upper", upper(col("name")))
    )
    DeltaTable.forPath(spark, silver).alias("t").merge(
        src.alias("s"), "t.id = s.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def optimize_vacuum(spark, silver):
    print("Maint   ▸ OPTIMIZE Silver")
    spark.sql(f"OPTIMIZE delta.`{silver}`")
    print("Maint   ▸ VACUUM Silver (RETAIN 0 HOURS)")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{silver}` RETAIN 0 HOURS")
    print("Maint   ▸ VACUUM complete")


def _env_flag(name: str, default: bool = False) -> bool:
    """Parsea flags de entorno tipo true/false/1/0."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _delete_path_if_exists(spark, path: str) -> None:
    """Borra una ruta (S3A/local) si existe para iniciar una demo limpia."""
    jvm = spark._jvm
    jsc = spark._jsc
    hadoop_conf = jsc.hadoopConfiguration()
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(hadoop_conf)
    if fs.exists(hpath):
        fs.delete(hpath, True)
        print(f"  Reset path ✓ {path}")


# ── Entry point ───────────────────────────────────────────────────────────
def run():
    spark = get_spark_session("Medallion_ETL")

    bucket = os.getenv("BUCKET_NAME", "demo-bucket")
    prefix = os.getenv("STORAGE_PREFIX", "s3a")
    bronze, silver, gold = paths(bucket, prefix)

    print(f"  paths → bronze={bronze}")
    print(f"  paths → silver={silver}")
    print(f"  paths → gold={gold}")

    try:
        if _env_flag("RESET_DEMO_DATA", default=True):
            _delete_path_if_exists(spark, bronze)
            _delete_path_if_exists(spark, silver)
            _delete_path_if_exists(spark, gold)
            spark.sql("DROP TABLE IF EXISTS sales.products_silver")
            spark.sql("DROP TABLE IF EXISTS sales.category_summary_gold")
            print("  Demo data reset complete ✓")

        ingest_bronze(spark, bronze)
        print("  Bronze OK ✓")

        process_silver(spark, bronze, silver)
        print("  Silver OK ✓")

        aggregate_gold(spark, silver, gold)
        print("  Gold OK ✓")

        simulate_append(spark, bronze)
        print("  Append OK ✓")

        merge_into_silver(spark, silver)
        print("  Merge OK ✓")

        # Evita bloqueos largos en entornos locales con S3/MinIO;
        # habilitar explícitamente si se requiere mantenimiento.
        if _env_flag("RUN_MAINTENANCE", default=False):
            optimize_vacuum(spark, silver)
            print("  Optimize+Vacuum OK ✓")
        else:
            print("  Maintenance skipped (set RUN_MAINTENANCE=true to enable)")

        spark.sql("SHOW TABLES IN sales").show()
        print("Done ✓")
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    run()
