"""
Databricks Local — shim público.

En un notebook, una sola llamada inyecta el contexto completo::

    from databricks_shim import inject_notebook_context
    inject_notebook_context()

Variables disponibles tras la inyección (igual que en Databricks real):

    spark    – SparkSession con Delta + Unity Catalog
    dbutils  – DBUtils shim (fs, secrets, widgets, notebook, jobs, data)
    display  – función display() compatible con Databricks
    sc       – SparkContext
    uc       – Unity Catalog shim (catálogos, schemas, volumes, grants, tags)

Uso directo de Unity Catalog::

    uc.sql("CREATE CATALOG IF NOT EXISTS analytics")
    uc.sql("CREATE VOLUME IF NOT EXISTS main.bronze.raw")
    uc.sql("GRANT SELECT ON TABLE main.bronze.products TO analyst@co.com")
    uc.sql("ALTER TABLE main.bronze.products SET TAGS ('env' = 'prod')")
    dbutils.fs.ls("/Volumes/main/bronze/raw/")
    dbutils.fs.put("dbfs:/tmp/test.txt", "hello", True)
"""

from databricks_shim.connect import get_spark_session
from databricks_shim.utils import get_dbutils, display
from databricks_shim.unity_catalog import UnityCatalogShim


def inject_notebook_context(app_name: str = "DatabricksLocal") -> None:
    """
    Inyecta ``spark``, ``dbutils``, ``display``, ``sc`` y ``uc``
    en el espacio global del notebook que llama a esta función.

    Replica el comportamiento de Databricks donde estas variables
    están disponibles sin imports explícitos.

    Llamar al inicio del notebook::

        from databricks_shim import inject_notebook_context
        inject_notebook_context()
    """
    import inspect

    spark = get_spark_session(app_name)
    dbutils = get_dbutils(spark)
    sc = spark.sparkContext
    uc = UnityCatalogShim(spark)

    frame = inspect.currentframe()
    if frame and frame.f_back:
        globs = frame.f_back.f_globals
        globs["spark"] = spark
        globs["dbutils"] = dbutils
        globs["display"] = display
        globs["sc"] = sc
        globs["uc"] = uc


__all__ = [
    "get_spark_session",
    "get_dbutils",
    "display",
    "inject_notebook_context",
    "UnityCatalogShim",
]
