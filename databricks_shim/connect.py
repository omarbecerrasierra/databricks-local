import os
import pathlib
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DatabricksLocal") -> SparkSession:
    """
    Devuelve una SparkSession local con PySpark 3.5.3 + Delta Lake 3.3.2.

    Todo corre 100% local — sin Docker, sin MinIO, sin PostgreSQL.

    Warehouse y metadatos se guardan en el filesystem:
      - ``.warehouse/main``            → warehouse del catálogo primario
      - ``.warehouse/hive_metastore``  → warehouse de legado
      - ``.volumes/``                  → Unity Catalog volumes
      - ``.dbfs/``                     → DBFS emulado

    Unity Catalog (tres niveles):
      - Catálogo ``main``            → DeltaCatalog primario
      - Catálogo ``hive_metastore``  → DeltaCatalog de legado (2 niveles)
    """
    print("⚡ Initializing Local Spark — PySpark 3.5.3 + Delta Lake 3.3.2 + Unity Catalog Emulator")

    # ── Builder base con extensiones Delta ───────────────────────────────────
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # ── Warehouse local ──────────────────────────────────────────────────────
    wh_base = os.getenv("LOCAL_WAREHOUSE", os.path.join(os.getcwd(), ".warehouse"))
    for cat in ("main", "hive_metastore"):
        pathlib.Path(os.path.join(wh_base, cat)).mkdir(parents=True, exist_ok=True)

    # Unity Catalog local — configuración de catálogos.
    #
    # PROBLEMA: Spark 3.5 + DeltaCatalog tiene el bug SPARK-47789.
    # Cuando se configura un catálogo personalizado (ej. 'main') como
    # DeltaCatalog via spark.conf.set(), su 'delegate' queda null y el
    # optimizer lanza NullPointerException incluso en escrituras simples.
    #
    # SOLUCIÓN: Solo usamos 'spark_catalog' como DeltaCatalog (única instancia
    # que Spark inicializa correctamente con su delegate = Hive/InMemory).
    # El catálogo 'main' del UC shim es VIRTUAL: el shim intercepta SQL de
    # tres niveles y lo enruta al 'spark_catalog'.
    builder = builder.config(
        "spark.sql.warehouse.dir", os.path.join(wh_base, "main")
    )

    # ── Delta JARs via pip ───────────────────────────────────────────────────
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass

    # ── Ajustes de rendimiento (replica defaults de Databricks Runtime) ───────
    builder = (
        builder.config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.session.timeZone", "UTC")
    )

    spark = builder.getOrCreate()

    # ── Inicializar registros de Unity Catalog ────────────────────────────────
    from databricks_shim.unity_catalog import init_unity_catalog

    init_unity_catalog(spark)

    return spark
