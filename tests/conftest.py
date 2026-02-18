"""
conftest.py — configuración global de pytest para DatabricksLocal.

Gestión de SparkSession entre módulos de test:
  Cada módulo de test crea su propia SparkSession independiente.
  Al finalizar cada módulo, la sesión se detiene para liberar recursos
  y evitar que el estado corrupto se propague al siguiente módulo.

Esto es necesario porque Spark 3.5 + DeltaCatalog tiene un bug conocido
(SPARK-47789) cuando se registran catálogos dinámicamente vía
spark.conf.set() en una sesión ya activa: la siguiente sesión reutilizaría
el estado corrupto con SparkSession.builder.getOrCreate().
"""


def pytest_runtest_setup(item):
    """Asegurar que no hay sesión Spark activa antes de cada módulo nuevo."""
    pass


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "spark_bug: marca tests que fallan por el bug SPARK-47789 "
        "(DeltaCatalog dinámico en Spark 3.5 sesión compartida)",
    )
