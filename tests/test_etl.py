import os
import pytest
from pathlib import Path

os.environ.setdefault("APP_ENV", "local")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from main import (
    ingest_bronze,
    process_silver,
    aggregate_gold,
    simulate_append,
)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import platform

    # Warehouse local único para el conjunto de tests
    wh = str(tmp_path_factory.mktemp("warehouse"))
    # Normalize path for cross-platform compatibility
    wh_normalized = wh.replace("\\", "/") if platform.system() == "Windows" else wh

    # Solo spark_catalog como DeltaCatalog — ver SPARK-47789.
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("TestETL")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", f"{wh_normalized}/main")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        )
    )

    builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    yield session
    session.stop()


def _bronze(tmp_path):
    return Path(tmp_path / "bronze").as_uri()


def _silver(tmp_path):
    return Path(tmp_path / "silver").as_uri()


def _gold(tmp_path):
    return Path(tmp_path / "gold").as_uri()


def test_bronze_ingestion(spark, tmp_path):
    bronze = _bronze(tmp_path)
    ingest_bronze(spark, bronze)
    df = spark.read.format("delta").load(bronze)
    assert df.count() == 5
    assert set(df.columns) == {"id", "name", "price", "category"}


def test_silver_processing(spark, tmp_path):
    bronze, silver = _bronze(tmp_path), _silver(tmp_path)
    ingest_bronze(spark, bronze)
    process_silver(spark, bronze, silver)
    df = spark.read.format("delta").load(silver)
    assert df.count() == 5
    assert "ingestion_time" in df.columns
    assert "name_upper" in df.columns
    assert df.filter(df.id == 1).first()["name_upper"] == "PRODUCT A"


def test_gold_aggregation(spark, tmp_path):
    bronze, silver, gold = _bronze(tmp_path), _silver(tmp_path), _gold(tmp_path)
    ingest_bronze(spark, bronze)
    process_silver(spark, bronze, silver)
    aggregate_gold(spark, silver, gold)
    df = spark.read.format("delta").load(gold)
    assert df.count() == 3
    assert "avg_price" in df.columns
    assert "product_count" in df.columns


def test_append_time_travel(spark, tmp_path):
    bronze = _bronze(tmp_path)
    ingest_bronze(spark, bronze)
    from delta.tables import DeltaTable

    v0 = DeltaTable.forPath(spark, bronze).toDF().count()
    simulate_append(spark, bronze)
    assert DeltaTable.forPath(spark, bronze).toDF().count() == v0 + 2
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(bronze)
    assert df_v0.count() == v0
