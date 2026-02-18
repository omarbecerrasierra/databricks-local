import os
import pathlib
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DatabricksLocal") -> SparkSession:
    """
    Devuelve una SparkSession configurada para el entorno actual.

    Modos:
      - Databricks cloud  : devuelve la sesión por defecto.
      - Docker            : JARs preinstalados + MinIO + PostgreSQL Hive Metastore.
      - Local / tests     : usa el paquete pip delta-spark.

    Unity Catalog (tres niveles):
      - Catálogo ``main``            → DeltaCatalog primario
      - Catálogo ``hive_metastore``  → DeltaCatalog de legado (2 niveles)
      - ``spark.sql.defaultCatalog`` = ``main``
    """
    env = os.getenv("APP_ENV", "local")

    if env != "local":
        return SparkSession.builder.appName(app_name).getOrCreate()

    print("⚡ Initializing Local Spark — Databricks 16.4 LTS + Unity Catalog Emulator")

    # ── Detectar Docker (JARs preinstalados) vs bare-metal ───────────────────
    jar_dir = "/opt/spark/jars"
    inside_docker = os.path.isdir(jar_dir) and os.path.exists(
        os.path.join(jar_dir, "delta-spark_2.12-3.3.2.jar")
    )

    # ── Builder base con extensiones Delta ───────────────────────────────────
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # ── S3A / MinIO ───────────────────────────────────────────────────────────
    endpoint    = os.getenv("AWS_ENDPOINT_URL")
    bucket_name = None   # definida aquí para que sea visible fuera del if
    if endpoint:
        bucket_name = os.getenv("BUCKET_NAME")
        if not bucket_name:
            raise ValueError(
                "AWS_ENDPOINT_URL está definida pero falta BUCKET_NAME. "
                "Defínela en tu .env o variables de entorno."
            )
        storage_prefix = os.getenv("STORAGE_PREFIX", "s3a")
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.access.key",
                    os.getenv("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key",
                    os.getenv("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.endpoint.region",
                    os.getenv("AWS_REGION", "us-east-1")) \
            .config("spark.delta.logStore.class",
                    "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

    # ── Hive Metastore (PostgreSQL) ───────────────────────────────────────────
    pg = os.getenv("POSTGRES_HOST")
    if pg or endpoint:
        # Activar Hive catalog cuando se usa PostgreSQL o S3/MinIO
        builder = builder.config("spark.sql.catalogImplementation", "hive")

    if pg:
        auto_create_metastore = os.getenv(
            "HIVE_METASTORE_AUTO_CREATE", "true"
        ).strip().lower() in {"1", "true", "yes", "y", "on"}
        builder = builder \
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:postgresql://{pg}:5432/{os.getenv('POSTGRES_DB')}") \
            .config("spark.hadoop.javax.jdo.option.ConnectionDriverName",
                    "org.postgresql.Driver") \
            .config("spark.hadoop.javax.jdo.option.ConnectionUserName",
                    os.getenv("POSTGRES_USER")) \
            .config("spark.hadoop.javax.jdo.option.ConnectionPassword",
                    os.getenv("POSTGRES_PASSWORD")) \
            .config(
                "spark.hadoop.datanucleus.schema.autoCreateAll",
                "true" if auto_create_metastore else "false",
            ) \
            .config("spark.hadoop.hive.metastore.schema.verification", "false")

    # ── Unity Catalog — catálogos múltiples (three-level namespace) ───────────
    #
    # Configura dos catálogos Delta:
    #   main            → catálogo primario UC  (spark.sql.defaultCatalog)
    #   hive_metastore  → catálogo de legado    (compatibilidad 2 niveles)
    #
    # Rutas del warehouse:
    #   S3/MinIO  →  s3a://bucket/warehouse/main  y  /hive_metastore
    #   Local     →  .warehouse/main              y  .warehouse/hive_metastore
    if endpoint and bucket_name:
        storage_prefix = os.getenv("STORAGE_PREFIX", "s3a")
        wh_base = f"{storage_prefix}://{bucket_name}/warehouse"
    else:
        local_wh = os.getenv("LOCAL_WAREHOUSE",
                              os.path.join(os.getcwd(), ".warehouse"))
        pathlib.Path(os.path.join(local_wh, "main")).mkdir(
            parents=True, exist_ok=True)
        pathlib.Path(os.path.join(local_wh, "hive_metastore")).mkdir(
            parents=True, exist_ok=True)
        wh_base = local_wh

    def _wh(cat: str) -> str:
        """Warehouse path para un catálogo dado."""
        if "://" in wh_base:           # S3 / MinIO
            return f"{wh_base}/{cat}"
        return os.path.join(wh_base, cat)

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
    #
    # Nota: spark.sql.defaultCatalog = "spark_catalog" (default de Spark).
    builder = builder \
        .config("spark.sql.warehouse.dir", _wh("main"))

    # ── JARs ──────────────────────────────────────────────────────────────────
    if inside_docker:
        builder = builder.config(
            "spark.jars",
            f"{jar_dir}/delta-spark_2.12-3.3.2.jar,"
            f"{jar_dir}/delta-storage-3.3.2.jar,"
            f"{jar_dir}/hadoop-aws-3.3.4.jar,"
            f"{jar_dir}/aws-java-sdk-bundle-1.12.262.jar,"
            f"{jar_dir}/postgresql-42.7.4.jar",
        )
    else:
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass

    # ── Ajustes de rendimiento (replica defaults de Databricks Runtime) ───────
    builder = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.compression.codec", "zstd") \
        .config("spark.sql.session.timeZone", "UTC")

    spark = builder.getOrCreate()

    # ── Inicializar registros de Unity Catalog ────────────────────────────────
    # Pasar el warehouse base S3 si estamos en modo Docker/MinIO,
    # para que el registro de catálogos refleje rutas S3 (no locales).
    from databricks_shim.unity_catalog import init_unity_catalog
    wh_s3_base = wh_base if "://" in wh_base else None
    init_unity_catalog(spark, warehouse_s3_base=wh_s3_base)

    return spark
