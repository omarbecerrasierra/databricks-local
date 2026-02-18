"""conftest.py — configuración global de pytest para DatabricksLocal."""

import os
import shutil
import pytest

# Forzar rutas temporales por test para evitar colisiones entre tests
_TEST_BASE = os.path.join(os.path.dirname(__file__), ".test_workspace")


@pytest.fixture(scope="session")
def spark():
    """SparkSession compartida para toda la sesión de tests."""
    os.environ.setdefault("LOCAL_WAREHOUSE", os.path.join(_TEST_BASE, ".warehouse"))
    os.environ.setdefault("VOLUMES_ROOT", os.path.join(_TEST_BASE, ".volumes"))
    os.environ.setdefault("DBFS_ROOT", os.path.join(_TEST_BASE, ".dbfs"))

    from databricks_shim import get_spark_session

    session = get_spark_session("TestSession")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def dbutils(spark):
    """DBUtilsShim compartido."""
    from databricks_shim import get_dbutils

    return get_dbutils(spark)


@pytest.fixture(scope="session")
def uc(spark):
    """UnityCatalogShim compartido."""
    from databricks_shim.unity_catalog import UnityCatalogShim

    return UnityCatalogShim(spark)


@pytest.fixture(autouse=True)
def _clean_test_artifacts():
    """Limpia artefactos de test al finalizar cada test."""
    yield
    # Cleanup de directorios temporales de tests individuales
    for d in (".test_tmp",):
        p = os.path.join(_TEST_BASE, d)
        if os.path.exists(p):
            shutil.rmtree(p, ignore_errors=True)


def pytest_sessionfinish(session, exitstatus):
    """Limpia el workspace de test al terminar la sesión."""
    if os.path.exists(_TEST_BASE):
        shutil.rmtree(_TEST_BASE, ignore_errors=True)
