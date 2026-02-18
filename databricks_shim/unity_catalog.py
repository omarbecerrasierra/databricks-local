"""
Databricks Unity Catalog local emulator.

Three-level namespace (catalog.schema.table):
  - Configurado como Delta Catalogs en Spark al arranque
  - Catálogos por defecto: ``main`` (primario) y ``hive_metastore`` (legado)
  - Catálogos personalizados creables con create_catalog() o CREATE CATALOG SQL

Volumes (/Volumes/catalog/schema/volume/path):
  - Se resuelven a <VOLUMES_ROOT>/catalog/schema/volume/path
  - VOLUMES_ROOT = $PWD/.volumes (local) o puede apuntar a s3a://...

DBFS (dbfs:/path):
  - Se resuelven a <DBFS_ROOT>/path
  - DBFS_ROOT = $PWD/.dbfs (local) o puede apuntar a s3a://...

SQL interceptado (no soportado nativamente por Spark):
  CREATE / DROP CATALOG  ·  DESCRIBE CATALOG
  ALTER CATALOG SET LOCATION / SET OWNER TO
  CREATE / DROP VOLUME  ·  DESCRIBE VOLUME  ·  SHOW VOLUMES [LIKE]
  GRANT / REVOKE [ON METASTORE|TABLE|VIEW|SCHEMA|CATALOG|VOLUME|FUNCTION|
      MODEL|PROCEDURE|CONNECTION|CLEAN ROOM|SERVICE CREDENTIAL|STORAGE CREDENTIAL|SHARE]
  SHOW GRANTS [ON type ref] [TO principal]
  ALTER ... SET / UNSET TAGS  ·  SHOW TAGS
  COMMENT ON TABLE / VOLUME / CATALOG / SCHEMA / VIEW
  ALTER TABLE / SCHEMA / VOLUME SET OWNER TO
  CREATE FOREIGN CATALOG (no-op — Lakehouse Federation)
  CREATE SHARE / RECIPIENT / EXTERNAL LOCATION / CREDENTIAL (no-op)

Uso típico en notebook::

    from databricks_shim import inject_notebook_context
    inject_notebook_context()          # inyecta spark, dbutils, display, sc, uc

    uc.sql("CREATE CATALOG IF NOT EXISTS analytics")
    uc.sql("CREATE VOLUME IF NOT EXISTS main.bronze.raw_files")
    uc.sql("GRANT SELECT ON TABLE main.bronze.products TO analyst@co.com")
    dbutils.fs.ls("/Volumes/main/bronze/raw_files/")
    dbutils.fs.put("dbfs:/tmp/hello.txt", "hello", True)
"""

from __future__ import annotations

import os
import re
import shutil
import pathlib
import datetime
from collections import namedtuple
from typing import Dict, List, Optional

# ── Named tuples que replican los tipos de retorno de Databricks ─────────────

CatalogInfo = namedtuple("CatalogInfo", ["name", "comment"])
SchemaInfo = namedtuple("SchemaInfo", ["catalog_name", "name", "comment"])
VolumeInfo = namedtuple(
    "VolumeInfo",
    ["catalog_name", "schema_name", "name", "volume_type", "storage_location"],
)
TableInfo = namedtuple(
    "TableInfo", ["catalog_name", "schema_name", "name", "table_type"]
)
ColumnInfo = namedtuple("ColumnInfo", ["name", "data_type", "nullable", "comment"])
GrantInfo = namedtuple(
    "GrantInfo", ["principal", "privilege", "object_type", "object_key"]
)
TagInfo = namedtuple("TagInfo", ["tag_name", "tag_value"])
FunctionInfo = namedtuple(
    "FunctionInfo", ["catalog_name", "schema_name", "name", "description"]
)
GroupInfo = namedtuple("GroupInfo", ["name", "members"])
DroppedTableInfo = namedtuple(
    "DroppedTableInfo", ["catalog_name", "schema_name", "name", "dropped_at"]
)
LineageInfo = namedtuple("LineageInfo", ["source", "target", "lineage_type"])


# ── Registros globales (singleton por proceso) ───────────────────────────────

_CATALOG_REGISTRY: Dict[str, str] = {}  # catalog_name → warehouse_path
_CURRENT_CATALOG: str = "main"
_CURRENT_SCHEMA: str = "default"
_TAG_STORE: Dict[str, Dict[str, str]] = {}  # "cat.sch.obj" → {tag: value}
_GRANT_LOG: List[Dict] = []  # audit log de grants
_VOLUME_REGISTRY: Dict[str, str] = {}  # "cat.sch.vol" → local_path
_FUNCTION_REGISTRY: Dict[str, Dict] = {}  # "cat.sch.func" → {info}
_GROUP_REGISTRY: Dict[str, List[str]] = {}  # group_name → [members]
_DROPPED_TABLES: List[Dict] = []  # audit log de tablas eliminadas
_LINEAGE_LOG: List[Dict] = []  # lineage tracking


# ── Helpers de rutas ─────────────────────────────────────────────────────────


def _volumes_root() -> str:
    return os.getenv("VOLUMES_ROOT", os.path.join(os.getcwd(), ".volumes"))


def _dbfs_root() -> str:
    return os.getenv("DBFS_ROOT", os.path.join(os.getcwd(), ".dbfs"))


def _warehouse_base() -> str:
    return os.getenv("LOCAL_WAREHOUSE", os.path.join(os.getcwd(), ".warehouse"))


def _join(root: str, rel: str) -> str:
    """Une root y rel de forma segura para rutas locales y s3a://."""
    return root.rstrip("/") + "/" + rel.lstrip("/")


def is_volume_path(path: str) -> bool:
    return path.startswith("/Volumes/")


def is_dbfs_path(path: str) -> bool:
    return path.startswith("dbfs:/")


def resolve_volume_path(path: str) -> str:
    """/Volumes/cat/sch/vol/subpath  →  <VOLUMES_ROOT>/cat/sch/vol/subpath"""
    if not is_volume_path(path):
        return path
    rel = path[len("/Volumes/") :]
    return _join(_volumes_root(), rel)


def resolve_dbfs_path(path: str) -> str:
    """dbfs:/path  →  <DBFS_ROOT>/path"""
    if not is_dbfs_path(path):
        return path
    rel = re.sub(r"^dbfs://+", "", path)
    rel = re.sub(r"^dbfs:/", "", rel)
    return _join(_dbfs_root(), rel)


# ── Patrones SQL para comandos UC no soportados por Spark ────────────────────
#
# Referencia oficial: docs.databricks.com (actualizado Feb 2026)
# ─────────────────────────────────────────────────────────────────────────────

_F = re.IGNORECASE | re.DOTALL

# CREATE CATALOG [IF NOT EXISTS] name [opciones en cualquier orden]
# Las opciones (MANAGED LOCATION, COMMENT, DEFAULT COLLATION) pueden aparecer
# en cualquier orden.  El regex solo detecta el comando; las opciones se
# extraen por separado con _extract_cat_opt() para ser order-independent.
_PAT_CREATE_CAT = re.compile(
    r"^CREATE\s+CATALOG\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?`?(?P<name>\w+)`?", _F
)


def _extract_cat_opt(query: str, keyword_re: str) -> "str | None":
    """Extrae el valor de una opción de CREATE CATALOG de forma order-independent."""
    m = re.search(keyword_re + r"\s+'([^']*)'", query, re.IGNORECASE)
    return m.group(1) if m else None


# CREATE FOREIGN CATALOG [IF NOT EXISTS] name USING CONNECTION ... → no-op
_PAT_CREATE_FOREIGN_CAT = re.compile(r"^CREATE\s+FOREIGN\s+CATALOG\b", _F)

# DROP CATALOG [IF EXISTS] name [CASCADE]
_PAT_DROP_CAT = re.compile(
    r"^DROP\s+CATALOG\s+(?P<ie>IF\s+EXISTS\s+)?`?(?P<name>\w+)`?"
    r"(?:\s+(?P<cascade>CASCADE))?",
    _F,
)

# ALTER CATALOG name SET OWNER TO principal
_PAT_ALTER_CAT_OWNER = re.compile(
    r"^ALTER\s+CATALOG\s+`?(?P<name>\w+)`?\s+SET\s+OWNER\s+TO\s+(?P<owner>\S+)", _F
)

# ALTER CATALOG name SET LOCATION 'path' (nueva en UC)
_PAT_ALTER_CAT_LOC = re.compile(
    r"^ALTER\s+CATALOG\s+`?(?P<name>\w+)`?\s+SET\s+LOCATION\s+'(?P<location>[^']*)'", _F
)

# DESCRIBE CATALOG name
_PAT_DESCRIBE_CAT = re.compile(
    r"^DESCRIBE\s+CATALOG\s+(?:EXTENDED\s+)?`?(?P<name>\w+)`?", _F
)

# CREATE [EXTERNAL] VOLUME [IF NOT EXISTS] cat.sch.vol [LOCATION 'path'] [COMMENT 'text']
# Nota: Databricks NO tiene "CREATE MANAGED VOLUME"; lo aceptamos por permisividad local
_PAT_CREATE_VOL = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?P<vtype>MANAGED|EXTERNAL)\s+)?VOLUME\s+"
    r"(?P<ine>IF\s+NOT\s+EXISTS\s+)?`?(?P<cat>\w+)`?\.`?(?P<sch>\w+)`?\.`?(?P<vol>\w+)`?"
    r"(?:\s+LOCATION\s+'(?P<loc>[^']*)')?(?:\s+COMMENT\s+'(?P<comment>[^']*)')?",
    _F,
)

# DROP VOLUME [IF EXISTS] cat.sch.vol
_PAT_DROP_VOL = re.compile(
    r"^DROP\s+VOLUME\s+(?P<ie>IF\s+EXISTS\s+)?`?(?P<cat>\w+)`?\.`?(?P<sch>\w+)`?\.`?(?P<vol>\w+)`?",
    _F,
)

# DESCRIBE VOLUME [EXTENDED] cat.sch.vol
_PAT_DESCRIBE_VOL = re.compile(
    r"^DESCRIBE\s+VOLUME\s+(?:EXTENDED\s+)?`?(?P<cat>\w+)`?\.`?(?P<sch>\w+)`?\.`?(?P<vol>\w+)`?",
    _F,
)

# SHOW VOLUMES [IN cat[.sch]] [LIKE 'pattern']
_PAT_SHOW_VOLS = re.compile(
    r"^SHOW\s+VOLUMES"
    r"(?:\s+IN\s+`?(?P<cat>\w+)`?(?:\.`?(?P<sch>\w+)`?)?)?"
    r"(?:\s+LIKE\s+'[^']*')?",
    _F,
)

# GRANT privilege ON securable_type [ref] TO principal
# Todos los securable objects de Unity Catalog (Jan 23, 2026)
_SECURABLE_TYPES = (
    r"METASTORE|TABLE|VIEW|MATERIALIZED\s+VIEW|SCHEMA|DATABASE|CATALOG"
    r"|VOLUME|FUNCTION|MODEL|PROCEDURE|CONNECTION|CLEAN\s+ROOM"
    r"|SHARE|RECIPIENT|PROVIDER"
    r"|ANY\s+FILE|EXTERNAL\s+LOCATION|EXTERNAL\s+METADATA"
    r"|SERVICE\s+CREDENTIAL|STORAGE\s+CREDENTIAL"
)

_PAT_GRANT = re.compile(
    r"^GRANT\s+(?P<priv>[\w\s,]+?)\s+ON\s+(?P<otype>" + _SECURABLE_TYPES + r")"
    r"(?:\s+(?P<oref>\S+))?\s+TO\s+(?P<principal>.+)",
    _F,
)

_PAT_REVOKE = re.compile(
    r"^REVOKE\s+(?P<priv>[\w\s,]+?)\s+ON\s+(?P<otype>" + _SECURABLE_TYPES + r")"
    r"(?:\s+(?P<oref>\S+))?\s+FROM\s+(?P<principal>.+)",
    _F,
)

# SHOW GRANTS [ON securable_type [ref]] [TO principal]
_PAT_SHOW_GRANTS = re.compile(
    r"^SHOW\s+GRANTS"
    r"(?:\s+ON\s+(?P<otype>\w+(?:\s+\w+)?)\s+(?P<oref>\S+))?"
    r"(?:\s+TO\s+(?P<principal>\S+))?",
    _F,
)

# ALTER TABLE/VIEW/SCHEMA/CATALOG/VOLUME/FUNCTION ref SET TAGS (k=v, ...)
_PAT_SET_TAGS = re.compile(
    r"^ALTER\s+(?:TABLE|VIEW|CATALOG|SCHEMA|DATABASE|VOLUME|FUNCTION|MATERIALIZED\s+VIEW)"
    r"\s+(?P<ref>\S+)\s+SET\s+TAGS\s*\((?P<tags>[^)]+)\)",
    _F,
)

_PAT_UNSET_TAGS = re.compile(
    r"^ALTER\s+(?:TABLE|VIEW|CATALOG|SCHEMA|DATABASE|VOLUME|FUNCTION|MATERIALIZED\s+VIEW)"
    r"\s+(?P<ref>\S+)\s+UNSET\s+TAGS\s*\((?P<keys>[^)]+)\)",
    _F,
)

_PAT_SHOW_TAGS = re.compile(r"^SHOW\s+TAGS\s+ON\s+(?:\w+\s+)?(?P<ref>\S+)", _F)

# COMMENT ON TABLE/VOLUME/CATALOG/SCHEMA ref IS 'text'
_PAT_COMMENT_ON = re.compile(
    r"^COMMENT\s+ON\s+(?P<otype>TABLE|VIEW|CATALOG|SCHEMA|VOLUME|FUNCTION)"
    r"\s+(?P<ref>\S+)\s+IS\s+'(?P<comment>[^']*)'",
    _F,
)

# ALTER TABLE/VIEW/SCHEMA ref SET OWNER TO principal
_PAT_SET_OWNER = re.compile(
    r"^ALTER\s+(?P<otype>TABLE|VIEW|SCHEMA|DATABASE|VOLUME|FUNCTION)\s+(?P<ref>\S+)"
    r"\s+SET\s+OWNER\s+TO\s+(?P<owner>\S+)",
    _F,
)

# No-op commands (Delta Sharing, External Locations, Storage Credentials, etc.)
_NOOP_PATTERNS = [
    (re.compile(r"^CREATE\s+SHARE\b", _F), "CREATE SHARE"),
    (re.compile(r"^DROP\s+SHARE\b", _F), "DROP SHARE"),
    (re.compile(r"^ALTER\s+SHARE\b", _F), "ALTER SHARE"),
    (re.compile(r"^SHOW\s+SHARES\b", _F), "SHOW SHARES"),
    (re.compile(r"^SHOW\s+ALL\s+IN\s+SHARE\b", _F), "SHOW ALL IN SHARE"),
    (re.compile(r"^CREATE\s+RECIPIENT\b", _F), "CREATE RECIPIENT"),
    (re.compile(r"^DROP\s+RECIPIENT\b", _F), "DROP RECIPIENT"),
    (re.compile(r"^ALTER\s+RECIPIENT\b", _F), "ALTER RECIPIENT"),
    (re.compile(r"^SHOW\s+RECIPIENTS\b", _F), "SHOW RECIPIENTS"),
    (re.compile(r"^DESCRIBE\s+RECIPIENT\b", _F), "DESCRIBE RECIPIENT"),
    (re.compile(r"^CREATE\s+EXTERNAL\s+LOCATION\b", _F), "CREATE EXTERNAL LOCATION"),
    (re.compile(r"^DROP\s+EXTERNAL\s+LOCATION\b", _F), "DROP EXTERNAL LOCATION"),
    (re.compile(r"^ALTER\s+EXTERNAL\s+LOCATION\b", _F), "ALTER EXTERNAL LOCATION"),
    (re.compile(r"^SHOW\s+EXTERNAL\s+LOCATIONS\b", _F), "SHOW EXTERNAL LOCATIONS"),
    (
        re.compile(r"^DESCRIBE\s+EXTERNAL\s+LOCATION\b", _F),
        "DESCRIBE EXTERNAL LOCATION",
    ),
    (re.compile(r"^CREATE\s+STORAGE\s+CREDENTIAL\b", _F), "CREATE STORAGE CREDENTIAL"),
    (re.compile(r"^DROP\s+STORAGE\s+CREDENTIAL\b", _F), "DROP STORAGE CREDENTIAL"),
    (re.compile(r"^ALTER\s+CREDENTIAL\b", _F), "ALTER CREDENTIAL"),
    (re.compile(r"^ALTER\s+STORAGE\s+CREDENTIAL\b", _F), "ALTER STORAGE CREDENTIAL"),
    (re.compile(r"^SHOW\s+STORAGE\s+CREDENTIALS\b", _F), "SHOW STORAGE CREDENTIALS"),
    (re.compile(r"^DESCRIBE\s+CREDENTIAL\b", _F), "DESCRIBE CREDENTIAL"),
    (
        re.compile(r"^DESCRIBE\s+STORAGE\s+CREDENTIAL\b", _F),
        "DESCRIBE STORAGE CREDENTIAL",
    ),
    (re.compile(r"^CREATE\s+SERVICE\s+CREDENTIAL\b", _F), "CREATE SERVICE CREDENTIAL"),
    (re.compile(r"^DROP\s+SERVICE\s+CREDENTIAL\b", _F), "DROP SERVICE CREDENTIAL"),
    (re.compile(r"^ALTER\s+SERVICE\s+CREDENTIAL\b", _F), "ALTER SERVICE CREDENTIAL"),
    (re.compile(r"^SHOW\s+SERVICE\s+CREDENTIALS\b", _F), "SHOW SERVICE CREDENTIALS"),
    (
        re.compile(r"^DESCRIBE\s+SERVICE\s+CREDENTIAL\b", _F),
        "DESCRIBE SERVICE CREDENTIAL",
    ),
    (re.compile(r"^CREATE\s+PROVIDER\b", _F), "CREATE PROVIDER"),
    (re.compile(r"^DROP\s+PROVIDER\b", _F), "DROP PROVIDER"),
    (re.compile(r"^ALTER\s+PROVIDER\b", _F), "ALTER PROVIDER"),
    (re.compile(r"^SHOW\s+PROVIDERS\b", _F), "SHOW PROVIDERS"),
    (re.compile(r"^DESCRIBE\s+PROVIDER\b", _F), "DESCRIBE PROVIDER"),
    (re.compile(r"^CREATE\s+CONNECTION\b", _F), "CREATE CONNECTION"),
    (re.compile(r"^DROP\s+CONNECTION\b", _F), "DROP CONNECTION"),
    (re.compile(r"^ALTER\s+CONNECTION\b", _F), "ALTER CONNECTION"),
    (re.compile(r"^SHOW\s+CONNECTIONS\b", _F), "SHOW CONNECTIONS"),
    (re.compile(r"^DESCRIBE\s+CONNECTION\b", _F), "DESCRIBE CONNECTION"),
    (re.compile(r"^CREATE\s+CLEAN\s+ROOM\b", _F), "CREATE CLEAN ROOM"),
    (re.compile(r"^DROP\s+CLEAN\s+ROOM\b", _F), "DROP CLEAN ROOM"),
    (re.compile(r"^ALTER\s+CLEAN\s+ROOM\b", _F), "ALTER CLEAN ROOM"),
    (re.compile(r"^DESCRIBE\s+CLEAN\s+ROOM\b", _F), "DESCRIBE CLEAN ROOM"),
    (re.compile(r"^SHOW\s+CLEAN\s+ROOMS\b", _F), "SHOW CLEAN ROOMS"),
    (re.compile(r"^REFRESH\s+FOREIGN\b", _F), "REFRESH FOREIGN"),
    (re.compile(r"^CREATE\s+MATERIALIZED\s+VIEW\b", _F), "CREATE MATERIALIZED VIEW"),
    (re.compile(r"^DROP\s+MATERIALIZED\s+VIEW\b", _F), "DROP MATERIALIZED VIEW"),
    (re.compile(r"^ALTER\s+MATERIALIZED\s+VIEW\b", _F), "ALTER MATERIALIZED VIEW"),
    (re.compile(r"^REFRESH\s+MATERIALIZED\s+VIEW\b", _F), "REFRESH MATERIALIZED VIEW"),
    (re.compile(r"^SHOW\s+MATERIALIZED\s+VIEWS\b", _F), "SHOW MATERIALIZED VIEWS"),
    (re.compile(r"^CREATE\s+STREAMING\s+TABLE\b", _F), "CREATE STREAMING TABLE"),
    (re.compile(r"^DROP\s+STREAMING\s+TABLE\b", _F), "DROP STREAMING TABLE"),
    (re.compile(r"^ALTER\s+STREAMING\s+TABLE\b", _F), "ALTER STREAMING TABLE"),
    (re.compile(r"^REFRESH\s+STREAMING\s+TABLE\b", _F), "REFRESH STREAMING TABLE"),
    (re.compile(r"^CREATE\s+SERVER\b", _F), "CREATE SERVER"),
    (re.compile(r"^DROP\s+SERVER\b", _F), "DROP SERVER"),
    (re.compile(r"^CREATE\s+PROCEDURE\b", _F), "CREATE PROCEDURE"),
    (re.compile(r"^DROP\s+PROCEDURE\b", _F), "DROP PROCEDURE"),
    (re.compile(r"^SHOW\s+PROCEDURES\b", _F), "SHOW PROCEDURES"),
    (re.compile(r"^DESCRIBE\s+PROCEDURE\b", _F), "DESCRIBE PROCEDURE"),
    (re.compile(r"^DESCRIBE\s+SHARE\b", _F), "DESCRIBE SHARE"),
    (re.compile(r"^SHOW\s+SHARES\s+IN\s+PROVIDER\b", _F), "SHOW SHARES IN PROVIDER"),
    (re.compile(r"^MSCK\s+REPAIR\s+PRIVILEGES\b", _F), "MSCK REPAIR PRIVILEGES"),
    (re.compile(r"^SYNC\b", _F), "SYNC"),
    (re.compile(r"^SET\s+RECIPIENT\b", _F), "SET RECIPIENT"),
]

# ── Patrones para nuevos comandos UC ─────────────────────────────────────────

# USE CATALOG name
_PAT_USE_CATALOG = re.compile(r"^USE\s+CATALOG\s+`?(?P<name>\w+)`?", _F)

# USE SCHEMA/DATABASE [catalog.]schema
_PAT_USE_SCHEMA = re.compile(
    r"^USE\s+(?:SCHEMA|DATABASE)\s+`?(?P<cat>\w+)`?(?:\.`?(?P<sch>\w+)`?)?", _F
)

# SHOW CATALOGS [LIKE 'pattern']
_PAT_SHOW_CATALOGS = re.compile(
    r"^SHOW\s+CATALOGS(?:\s+LIKE\s+'(?P<pattern>[^']*)')?", _F
)

# CREATE SCHEMA/DATABASE [IF NOT EXISTS] [catalog.]schema [COMMENT '...']
_PAT_CREATE_SCHEMA = re.compile(
    r"^CREATE\s+(?:SCHEMA|DATABASE)\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?"
    r"`?(?P<ref>[\w.`]+)`?"
    r"(?:\s+COMMENT\s+'(?P<comment>[^']*)')?"
    r"(?:\s+MANAGED\s+LOCATION\s+'(?P<location>[^']*)')?",
    _F,
)

# DROP SCHEMA/DATABASE [IF EXISTS] [catalog.]schema [CASCADE|RESTRICT]
_PAT_DROP_SCHEMA = re.compile(
    r"^DROP\s+(?:SCHEMA|DATABASE)\s+(?P<ie>IF\s+EXISTS\s+)?"
    r"`?(?P<ref>[\w.`]+)`?"
    r"(?:\s+(?P<cascade>CASCADE|RESTRICT))?",
    _F,
)

# DESCRIBE SCHEMA/DATABASE [EXTENDED] [catalog.]schema
_PAT_DESCRIBE_SCHEMA = re.compile(
    r"^DESCRIBE\s+(?:SCHEMA|DATABASE)\s+(?:EXTENDED\s+)?`?(?P<ref>[\w.`]+)`?", _F
)

# CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name (...) RETURNS type ...
_PAT_CREATE_FUNCTION = re.compile(
    r"^CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?"
    r"`?(?P<ref>[\w.`]+)`?",
    _F,
)

# DROP FUNCTION [IF EXISTS] name
_PAT_DROP_FUNCTION = re.compile(
    r"^DROP\s+(?:TEMPORARY\s+)?FUNCTION\s+(?P<ie>IF\s+EXISTS\s+)?"
    r"`?(?P<ref>[\w.`]+)`?",
    _F,
)

# DESCRIBE FUNCTION [EXTENDED] name
_PAT_DESCRIBE_FUNCTION = re.compile(
    r"^DESCRIBE\s+FUNCTION\s+(?:EXTENDED\s+)?`?(?P<ref>[\w.`]+)`?", _F
)

# SHOW FUNCTIONS [IN catalog.schema] [LIKE 'pattern']
_PAT_SHOW_FUNCTIONS = re.compile(
    r"^SHOW\s+(?:ALL\s+|USER\s+|SYSTEM\s+)?FUNCTIONS"
    r"(?:\s+(?:IN|FROM)\s+`?(?P<ref>[\w.`]+)`?)?"
    r"(?:\s+LIKE\s+'(?P<pattern>[^']*)')?",
    _F,
)

# SHOW TABLES [IN catalog.schema] [LIKE 'pattern']
_PAT_SHOW_TABLES = re.compile(
    r"^SHOW\s+TABLES"
    r"(?:\s+(?:IN|FROM)\s+`?(?P<ref>[\w.`]+)`?)?"
    r"(?:\s+LIKE\s+'(?P<pattern>[^']*)')?",
    _F,
)

# SHOW VIEWS [IN catalog.schema] [LIKE 'pattern']
_PAT_SHOW_VIEWS = re.compile(
    r"^SHOW\s+VIEWS"
    r"(?:\s+(?:IN|FROM)\s+`?(?P<ref>[\w.`]+)`?)?"
    r"(?:\s+LIKE\s+'(?P<pattern>[^']*)')?",
    _F,
)

# SHOW COLUMNS IN table_name [IN schema]
_PAT_SHOW_COLUMNS = re.compile(
    r"^SHOW\s+COLUMNS\s+(?:IN|FROM)\s+`?(?P<ref>[\w.`]+)`?", _F
)

# SHOW CREATE TABLE name
_PAT_SHOW_CREATE_TABLE = re.compile(
    r"^SHOW\s+CREATE\s+TABLE\s+`?(?P<ref>[\w.`]+)`?", _F
)

# SHOW SCHEMAS/DATABASES [IN catalog] [LIKE 'pattern']
_PAT_SHOW_SCHEMAS = re.compile(
    r"^SHOW\s+(?:SCHEMAS|DATABASES)"
    r"(?:\s+(?:IN|FROM)\s+`?(?P<cat>\w+)`?)?"
    r"(?:\s+LIKE\s+'(?P<pattern>[^']*)')?",
    _F,
)

# UNDROP TABLE name
_PAT_UNDROP = re.compile(r"^UNDROP\s+TABLE\s+`?(?P<ref>[\w.`]+)`?", _F)

# SHOW TABLES DROPPED [IN schema]
_PAT_SHOW_DROPPED = re.compile(
    r"^SHOW\s+TABLES\s+DROPPED" r"(?:\s+(?:IN|FROM)\s+`?(?P<ref>[\w.`]+)`?)?", _F
)

# DENY privilege ON type ref TO principal
_PAT_DENY = re.compile(
    r"^DENY\s+(?P<priv>[\w\s,]+?)\s+ON\s+(?P<otype>" + _SECURABLE_TYPES + r")"
    r"(?:\s+(?P<oref>\S+))?\s+TO\s+(?P<principal>.+)",
    _F,
)

# GRANT ON SHARE share TO recipient / REVOKE ON SHARE share FROM recipient
_PAT_GRANT_ON_SHARE = re.compile(
    r"^GRANT\s+(?P<priv>[\w\s,]+?)\s+ON\s+SHARE\s+(?P<share>\S+)\s+TO\s+(?P<principal>.+)",
    _F,
)
_PAT_REVOKE_ON_SHARE = re.compile(
    r"^REVOKE\s+(?P<priv>[\w\s,]+?)\s+ON\s+SHARE\s+(?P<share>\S+)\s+FROM\s+(?P<principal>.+)",
    _F,
)

# SHOW GRANTS ON SHARE share / SHOW GRANTS TO RECIPIENT recipient
_PAT_SHOW_GRANTS_ON_SHARE = re.compile(
    r"^SHOW\s+GRANTS\s+ON\s+SHARE\s+(?P<share>\S+)", _F
)
_PAT_SHOW_GRANTS_TO_RECIPIENT = re.compile(
    r"^SHOW\s+GRANTS\s+TO\s+RECIPIENT\s+(?P<recipient>\S+)", _F
)

# CREATE/DROP/ALTER GROUP
_PAT_CREATE_GROUP = re.compile(
    r"^CREATE\s+GROUP\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?`?(?P<name>\w+)`?", _F
)
_PAT_DROP_GROUP = re.compile(
    r"^DROP\s+GROUP\s+(?P<ie>IF\s+EXISTS\s+)?`?(?P<name>\w+)`?", _F
)
_PAT_ALTER_GROUP = re.compile(
    r"^ALTER\s+GROUP\s+`?(?P<name>\w+)`?\s+(?P<action>ADD|REMOVE)\s+(?:USER|MEMBER)\s+(?P<member>.+)",
    _F,
)
_PAT_SHOW_GROUPS = re.compile(r"^SHOW\s+GROUPS", _F)
_PAT_SHOW_USERS = re.compile(r"^SHOW\s+USERS", _F)

# ALTER VOLUME cat.sch.vol SET OWNER TO / RENAME TO etc.
_PAT_ALTER_VOLUME = re.compile(
    r"^ALTER\s+VOLUME\s+`?(?P<ref>[\w.`]+)`?\s+(?P<action>SET\s+OWNER\s+TO|RENAME\s+TO|SET\s+TAGS|UNSET\s+TAGS)",
    _F,
)

# GET / PUT INTO / REMOVE (resource management for volumes)
_PAT_GET_FILE = re.compile(r"^GET\s+'(?P<src>[^']+)'\s+TO\s+'(?P<dst>[^']+)'", _F)
_PAT_PUT_FILE = re.compile(r"^PUT\s+'(?P<src>[^']+)'\s+INTO\s+'(?P<dst>[^']+)'", _F)
_PAT_REMOVE_FILE = re.compile(r"^REMOVE\s+'(?P<path>[^']+)'", _F)

# LIST path
_PAT_LIST = re.compile(r"^LIST\s+'(?P<path>[^']+)'", _F)


# ── Clase principal ───────────────────────────────────────────────────────────


class UnityCatalogShim:
    """
    Shim de alta fidelidad para Databricks Unity Catalog en entornos locales.

    Cubre:
      - Espacio de nombres de tres niveles (catalog.schema.table)
      - Catálogos múltiples: ``main``, ``hive_metastore`` y personalizados
      - Volumes (/Volumes/…) mapeados al sistema de archivos local
      - DBFS (dbfs:/…) mapeados al sistema de archivos local
      - Grants / Revoke (no-op con log de auditoría)
      - Tags en memoria
      - Interceptor SQL para DDL UC específico
      - Row filters / Column masks (no-op)
      - Delta Sharing (no-op)
    """

    def __init__(self, spark):
        self._spark = spark
        self._tags = _TAG_STORE
        self._catalogs = _CATALOG_REGISTRY
        self._grants = _GRANT_LOG
        self._volumes = _VOLUME_REGISTRY

    # ── Interceptor SQL ───────────────────────────────────────────────────────

    def sql(self, query: str, *args, **kwargs):
        """
        Ejecuta SQL con extensiones de Unity Catalog.

        Intercepta comandos UC no soportados por Spark (CREATE CATALOG,
        GRANT, CREATE VOLUME, SET TAGS, etc.) y delega el resto a spark.sql().
        """
        # Normalizar: quitar espacios y punto y coma final (SQL interactivo)
        q = query.strip().rstrip(";").strip()

        # CREATE FOREIGN CATALOG (no-op — requiere Lakehouse Federation)
        if _PAT_CREATE_FOREIGN_CAT.match(q):
            print(
                "[Unity] CREATE FOREIGN CATALOG — no-op localmente "
                "(requiere Lakehouse Federation)"
            )
            return None

        # CREATE CATALOG — opciones se extraen de forma order-independent
        m = _PAT_CREATE_CAT.match(q)
        if m:
            return self.create_catalog(
                m.group("name"),
                comment=_extract_cat_opt(q, r"COMMENT") or "",
                location=_extract_cat_opt(q, r"MANAGED\s+LOCATION"),
                if_not_exists=bool(m.group("ine")),
            )

        # DROP CATALOG
        m = _PAT_DROP_CAT.match(q)
        if m:
            return self.drop_catalog(
                m.group("name"),
                if_exists=bool(m.group("ie")),
                cascade=bool(m.group("cascade")),
            )

        # ALTER CATALOG SET OWNER
        m = _PAT_ALTER_CAT_OWNER.match(q)
        if m:
            print(
                f"[Unity] ALTER CATALOG {m.group('name')} SET OWNER "
                f"TO {m.group('owner')} (no-op locally)"
            )
            return None

        # ALTER CATALOG SET LOCATION (nuevo en UC)
        m = _PAT_ALTER_CAT_LOC.match(q)
        if m:
            cat = m.group("name")
            loc = m.group("location")
            if cat in self._catalogs:
                self._catalogs[cat] = loc
            print(f"[Unity] ALTER CATALOG '{cat}' SET LOCATION '{loc}'")
            return None

        # DESCRIBE CATALOG
        m = _PAT_DESCRIBE_CAT.match(q)
        if m:
            name = m.group("name")
            path = self._catalogs.get(name, "unknown")
            rows = [
                ("name", name),
                ("catalog_type", "MANAGED"),
                ("storage_location", path),
                ("comment", ""),
            ]
            return self._spark.createDataFrame(rows, "key STRING, value STRING")

        # DESCRIBE VOLUME
        m = _PAT_DESCRIBE_VOL.match(q)
        if m:
            key = f"{m.group('cat')}.{m.group('sch')}.{m.group('vol')}"
            path = self._volumes.get(key, "unknown")
            rows = [
                ("name", m.group("vol")),
                ("catalog_name", m.group("cat")),
                ("schema_name", m.group("sch")),
                ("volume_type", "MANAGED"),
                ("storage_location", path),
                ("comment", ""),
            ]
            return self._spark.createDataFrame(rows, "key STRING, value STRING")

        # CREATE VOLUME
        m = _PAT_CREATE_VOL.match(q)
        if m:
            return self.create_volume(
                m.group("cat"),
                m.group("sch"),
                m.group("vol"),
                volume_type=(m.group("vtype") or "MANAGED").upper(),
                location=m.group("loc"),
                comment=m.group("comment") or "",
                if_not_exists=bool(m.group("ine")),
            )

        # DROP VOLUME
        m = _PAT_DROP_VOL.match(q)
        if m:
            return self.drop_volume(
                m.group("cat"),
                m.group("sch"),
                m.group("vol"),
                if_exists=bool(m.group("ie")),
            )

        # SHOW VOLUMES
        m = _PAT_SHOW_VOLS.match(q)
        if m:
            vols = self.list_volumes(
                catalog=m.group("cat"),
                schema=m.group("sch"),
            )
            rows = [
                (
                    v.catalog_name,
                    v.schema_name,
                    v.name,
                    v.volume_type,
                    v.storage_location,
                )
                for v in vols
            ]
            schema_str = (
                "catalog_name STRING, schema_name STRING, "
                "name STRING, volume_type STRING, storage_location STRING"
            )
            return (
                self._spark.createDataFrame(rows, schema_str)
                if rows
                else self._spark.createDataFrame([], schema_str)
            )

        # ── DENY ─────────────────────────────────────────────────────────
        m = _PAT_DENY.match(q)
        if m:
            return self.deny(
                m.group("priv").strip(),
                m.group("otype").strip(),
                (m.group("oref") or "").strip(),
                m.group("principal").strip(),
            )

        # GRANT  – oref puede ser None cuando el securable es METASTORE
        m = _PAT_GRANT.match(q)
        if m:
            return self.grant(
                m.group("priv").strip(),
                m.group("otype").strip(),
                (m.group("oref") or "").strip(),  # METASTORE no lleva ref
                m.group("principal").strip(),
            )

        # REVOKE – igual que GRANT
        m = _PAT_REVOKE.match(q)
        if m:
            return self.revoke(
                m.group("priv").strip(),
                m.group("otype").strip(),
                (m.group("oref") or "").strip(),  # METASTORE no lleva ref
                m.group("principal").strip(),
            )

        # ── GRANT/REVOKE ON SHARE (before general SHOW GRANTS) ─────────────
        m = _PAT_GRANT_ON_SHARE.match(q)
        if m:
            return self.grant(
                m.group("priv").strip(),
                "SHARE",
                m.group("share").strip(),
                m.group("principal").strip(),
            )
        m = _PAT_REVOKE_ON_SHARE.match(q)
        if m:
            return self.revoke(
                m.group("priv").strip(),
                "SHARE",
                m.group("share").strip(),
                m.group("principal").strip(),
            )

        # ── SHOW GRANTS ON SHARE / TO RECIPIENT (before general SHOW GRANTS) ─
        m = _PAT_SHOW_GRANTS_ON_SHARE.match(q)
        if m:
            grants = self.show_grants(object_ref=m.group("share").strip())
            rows = [
                (g.principal, g.privilege, g.object_type, g.object_key) for g in grants
            ]
            schema_str = (
                "principal STRING, privilege STRING, "
                "object_type STRING, object_key STRING"
            )
            return (
                self._spark.createDataFrame(rows, schema_str)
                if rows
                else self._spark.createDataFrame([], schema_str)
            )
        m = _PAT_SHOW_GRANTS_TO_RECIPIENT.match(q)
        if m:
            grants = self.show_grants(principal=m.group("recipient").strip())
            rows = [
                (g.principal, g.privilege, g.object_type, g.object_key) for g in grants
            ]
            schema_str = (
                "principal STRING, privilege STRING, "
                "object_type STRING, object_key STRING"
            )
            return (
                self._spark.createDataFrame(rows, schema_str)
                if rows
                else self._spark.createDataFrame([], schema_str)
            )

        # SHOW GRANTS [ON type ref] [TO principal]
        m = _PAT_SHOW_GRANTS.match(q)
        if m:
            schema_str = (
                "principal STRING, privilege STRING, "
                "object_type STRING, object_key STRING"
            )
            rows = [
                (g["principal"], g["privilege"], g["object_type"], g["object_key"])
                for g in self._grants
            ]
            oref = m.group("oref")
            if oref:
                rows = [r for r in rows if r[3] == oref]
            # Filtro TO principal (SHOW GRANTS ON type ref TO principal)
            principal_filter = m.group("principal")
            if principal_filter:
                rows = [r for r in rows if r[0] == principal_filter]
            return (
                self._spark.createDataFrame(rows, schema_str)
                if rows
                else self._spark.createDataFrame([], schema_str)
            )

        # ALTER ... SET TAGS
        m = _PAT_SET_TAGS.match(q)
        if m:
            self.set_tags(m.group("ref"), _parse_tags(m.group("tags")))
            return None

        # ALTER ... UNSET TAGS
        m = _PAT_UNSET_TAGS.match(q)
        if m:
            keys = [k.strip().strip("'`\"") for k in m.group("keys").split(",")]
            self.unset_tags(m.group("ref"), keys)
            return None

        # SHOW TAGS
        m = _PAT_SHOW_TAGS.match(q)
        if m:
            tags = self.get_tags(m.group("ref"))
            rows = list(tags.items())
            schema_str = "tag_name STRING, tag_value STRING"
            return (
                self._spark.createDataFrame(rows, schema_str)
                if rows
                else self._spark.createDataFrame([], schema_str)
            )

        # COMMENT ON TABLE/VOLUME/... ref IS 'text' → almacena como tag
        m = _PAT_COMMENT_ON.match(q)
        if m:
            self.set_tags(m.group("ref"), {"_comment": m.group("comment")})
            print(
                f"[Unity] COMMENT ON {m.group('otype')} "
                f"{m.group('ref')}: {m.group('comment')}"
            )
            return None

        # ALTER TABLE/SCHEMA/VOLUME ... SET OWNER TO principal (no-op)
        m = _PAT_SET_OWNER.match(q)
        if m:
            print(
                f"[Unity] ALTER {m.group('otype')} {m.group('ref')} "
                f"SET OWNER TO {m.group('owner')} (no-op locally)"
            )
            return None

        # No-op UC commands (Sharing, External Locations, Credentials, etc.)
        for pat, label in _NOOP_PATTERNS:
            if pat.match(q):
                print(f"[Unity] {label} — no-op locally")
                return self._spark.createDataFrame([], "result STRING")

        # ── USE CATALOG ──────────────────────────────────────────────────────
        m = _PAT_USE_CATALOG.match(q)
        if m:
            self.set_current_catalog(m.group("name"))
            return None

        # ── USE SCHEMA/DATABASE ──────────────────────────────────────────────
        m = _PAT_USE_SCHEMA.match(q)
        if m:
            cat = m.group("cat")
            sch = m.group("sch")
            if sch:
                self.set_current_catalog(cat)
                self.set_current_schema(sch)
            else:
                # Solo se pasó un nombre → es schema, no catálogo
                self.set_current_schema(cat)
            return None

        # ── SHOW CATALOGS ────────────────────────────────────────────────────
        m = _PAT_SHOW_CATALOGS.match(q)
        if m:
            pattern = m.group("pattern")
            cats = self.list_catalogs()
            if pattern:
                import fnmatch

                cats = [c for c in cats if fnmatch.fnmatch(c.name, pattern)]
            rows = [(c.name,) for c in cats]
            return (
                self._spark.createDataFrame(rows, "catalog STRING")
                if rows
                else self._spark.createDataFrame([], "catalog STRING")
            )

        # ── CREATE SCHEMA/DATABASE via interceptor ───────────────────────────
        m = _PAT_CREATE_SCHEMA.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            parts = ref.split(".")
            if len(parts) == 2:
                cat, sch = parts
            else:
                cat, sch = _CURRENT_CATALOG, parts[0]
            comment = m.group("comment") or ""
            return self.create_schema(
                cat, sch, comment=comment, if_not_exists=bool(m.group("ine"))
            )

        # ── DROP SCHEMA/DATABASE via interceptor ─────────────────────────────
        m = _PAT_DROP_SCHEMA.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            parts = ref.split(".")
            if len(parts) == 2:
                cat, sch = parts
            else:
                cat, sch = _CURRENT_CATALOG, parts[0]
            return self.drop_schema(
                cat,
                sch,
                if_exists=bool(m.group("ie")),
                cascade=(
                    m.group("cascade") == "CASCADE" if m.group("cascade") else False
                ),
            )

        # ── DESCRIBE SCHEMA/DATABASE ─────────────────────────────────────────
        m = _PAT_DESCRIBE_SCHEMA.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            parts = ref.split(".")
            if len(parts) == 2:
                cat, sch = parts
            else:
                cat, sch = _CURRENT_CATALOG, parts[0]
            return self.describe_schema(cat, sch)

        # ── SHOW SCHEMAS/DATABASES ───────────────────────────────────────────
        m = _PAT_SHOW_SCHEMAS.match(q)
        if m:
            cat = m.group("cat") or _CURRENT_CATALOG
            schemas = self.list_schemas(cat)
            rows = [(s.name,) for s in schemas]
            return (
                self._spark.createDataFrame(rows, "databaseName STRING")
                if rows
                else self._spark.createDataFrame([], "databaseName STRING")
            )

        # ── CREATE FUNCTION ──────────────────────────────────────────────────
        m = _PAT_CREATE_FUNCTION.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            parts = ref.split(".")
            if len(parts) == 3:
                cat, sch, func = parts
            elif len(parts) == 2:
                cat, sch, func = _CURRENT_CATALOG, parts[0], parts[1]
            else:
                cat, sch, func = _CURRENT_CATALOG, "default", parts[0]
            # Try delegating to Spark first (SQL/Python UDFs)
            try:
                return self._spark.sql(query)
            except Exception:
                # Register in shim for UC-only functions
                return self.create_function(
                    cat, sch, func, definition=q, if_not_exists=bool(m.group("ine"))
                )

        # ── DROP FUNCTION ────────────────────────────────────────────────────
        m = _PAT_DROP_FUNCTION.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            parts = ref.split(".")
            if len(parts) == 3:
                cat, sch, func = parts
            elif len(parts) == 2:
                cat, sch, func = _CURRENT_CATALOG, parts[0], parts[1]
            else:
                cat, sch, func = _CURRENT_CATALOG, "default", parts[0]
            # Try Spark first
            try:
                return self._spark.sql(query)
            except Exception:
                return self.drop_function(cat, sch, func, if_exists=bool(m.group("ie")))

        # ── DESCRIBE FUNCTION ────────────────────────────────────────────────
        m = _PAT_DESCRIBE_FUNCTION.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            try:
                return self._spark.sql(query)
            except Exception:
                return self.describe_function_sql(ref)

        # ── SHOW FUNCTIONS ───────────────────────────────────────────────────
        m = _PAT_SHOW_FUNCTIONS.match(q)
        if m:
            try:
                return self._spark.sql(query)
            except Exception:
                ref = m.group("ref")
                if ref:
                    parts = ref.replace("`", "").split(".")
                    cat = parts[0] if len(parts) >= 2 else _CURRENT_CATALOG
                    sch = parts[1] if len(parts) >= 2 else parts[0]
                else:
                    cat, sch = _CURRENT_CATALOG, "default"
                funcs = self.list_functions(cat, sch)
                rows = [(f.catalog_name, f.schema_name, f.name) for f in funcs]
                schema_str = "catalog STRING, schema STRING, function STRING"
                return (
                    self._spark.createDataFrame(rows, schema_str)
                    if rows
                    else self._spark.createDataFrame([], schema_str)
                )

        # ── UNDROP TABLE (must be before SHOW TABLES) ─────────────────────
        m = _PAT_UNDROP.match(q)
        if m:
            ref = m.group("ref").replace("`", "")
            return self.undrop_table(ref)

        # ── SHOW TABLES DROPPED (must be before SHOW TABLES) ─────────────────
        m = _PAT_SHOW_DROPPED.match(q)
        if m:
            rows = [
                (d["catalog"], d["schema"], d["table"], d["dropped_at"])
                for d in _DROPPED_TABLES
            ]
            schema_str = (
                "catalog STRING, schema STRING, " "tableName STRING, droppedAt STRING"
            )
            return (
                self._spark.createDataFrame(rows, schema_str)
                if rows
                else self._spark.createDataFrame([], schema_str)
            )

        # ── SHOW TABLES ──────────────────────────────────────────────────────
        m = _PAT_SHOW_TABLES.match(q)
        if m:
            ref = m.group("ref")
            if ref:
                parts = ref.replace("`", "").split(".")
                if len(parts) == 2:
                    cat, sch = parts
                else:
                    cat, sch = _CURRENT_CATALOG, parts[0]
            else:
                cat, sch = _CURRENT_CATALOG, _CURRENT_SCHEMA
            try:
                return self._spark.sql(query)
            except Exception:
                tables = self.list_tables(cat, sch)
                rows = [(sch, t.name, False) for t in tables]
                schema_str = "database STRING, tableName STRING, isTemporary BOOLEAN"
                return (
                    self._spark.createDataFrame(rows, schema_str)
                    if rows
                    else self._spark.createDataFrame([], schema_str)
                )

        # ── SHOW VIEWS ───────────────────────────────────────────────────────
        m = _PAT_SHOW_VIEWS.match(q)
        if m:
            try:
                return self._spark.sql(query)
            except Exception:
                return self._spark.createDataFrame(
                    [], "namespace STRING, viewName STRING, isTemporary BOOLEAN"
                )

        # ── SHOW COLUMNS ─────────────────────────────────────────────────────
        m = _PAT_SHOW_COLUMNS.match(q)
        if m:
            try:
                return self._spark.sql(query)
            except Exception:
                return self._spark.createDataFrame([], "col_name STRING")

        # ── SHOW CREATE TABLE ────────────────────────────────────────────────
        m = _PAT_SHOW_CREATE_TABLE.match(q)
        if m:
            try:
                return self._spark.sql(query)
            except Exception:
                return self._spark.createDataFrame(
                    [("-- DDL not available locally",)], "createtab_stmt STRING"
                )

        # ── CREATE/DROP/ALTER GROUP ──────────────────────────────────────────
        m = _PAT_CREATE_GROUP.match(q)
        if m:
            return self.create_group(
                m.group("name"), if_not_exists=bool(m.group("ine"))
            )
        m = _PAT_DROP_GROUP.match(q)
        if m:
            return self.drop_group(m.group("name"), if_exists=bool(m.group("ie")))
        m = _PAT_ALTER_GROUP.match(q)
        if m:
            name = m.group("name")
            member = m.group("member").strip().strip("'`\"")
            if m.group("action").upper() == "ADD":
                return self.add_group_member(name, member)
            else:
                return self.remove_group_member(name, member)

        m = _PAT_SHOW_GROUPS.match(q)
        if m:
            groups = self.list_groups()
            rows = [(g.name,) for g in groups]
            return (
                self._spark.createDataFrame(rows, "name STRING")
                if rows
                else self._spark.createDataFrame([], "name STRING")
            )

        m = _PAT_SHOW_USERS.match(q)
        if m:
            user = os.getenv("DATABRICKS_USER", os.getenv("USER", "local-user"))
            return self._spark.createDataFrame([(user,)], "name STRING")

        # ── ALTER VOLUME ─────────────────────────────────────────────────────
        m = _PAT_ALTER_VOLUME.match(q)
        if m:
            # SET TAGS and UNSET TAGS are handled above; SET OWNER is no-op
            action = m.group("action").upper()
            ref = m.group("ref").replace("`", "")
            if "SET OWNER" in action:
                print(f"[Unity] ALTER VOLUME {ref} SET OWNER TO ... (no-op locally)")
            elif "RENAME" in action:
                print(f"[Unity] ALTER VOLUME {ref} RENAME TO ... (no-op locally)")
            return None

        # ── GET / PUT INTO / REMOVE (volume resource mgmt) ──────────────────
        m = _PAT_GET_FILE.match(q)
        if m:
            print(
                f"[Unity] GET '{m.group('src')}' TO '{m.group('dst')}' (no-op locally)"
            )
            return None
        m = _PAT_PUT_FILE.match(q)
        if m:
            print(
                f"[Unity] PUT '{m.group('src')}' INTO '{m.group('dst')}' (no-op locally)"
            )
            return None
        m = _PAT_REMOVE_FILE.match(q)
        if m:
            print(f"[Unity] REMOVE '{m.group('path')}' (no-op locally)")
            return None

        # ── LIST path ────────────────────────────────────────────────────────
        m = _PAT_LIST.match(q)
        if m:
            path = m.group("path")
            from databricks_shim.utils import FSMock

            fs = FSMock(spark=self._spark)
            try:
                files = fs.ls(path)
                rows = [(f.path, f.name, f.size) for f in files]
                return self._spark.createDataFrame(
                    rows, "path STRING, name STRING, size LONG"
                )
            except FileNotFoundError:
                return self._spark.createDataFrame(
                    [], "path STRING, name STRING, size LONG"
                )

        # Todo lo demás → Spark SQL nativo
        return self._spark.sql(query, *args, **kwargs)

    # ── Gestión de catálogos ──────────────────────────────────────────────────

    def create_catalog(
        self,
        name: str,
        comment: str = "",
        location: "str | None" = None,
        if_not_exists: bool = False,
        properties: Optional[Dict] = None,
    ) -> None:
        """Crea un nuevo catálogo (configura un DeltaCatalog en Spark).

        Args:
            name:          Nombre del catálogo.
            comment:       Descripción opcional (COMMENT).
            location:      Ruta gestionada personalizada (MANAGED LOCATION).
                           Si es None, usa el warehouse por defecto.
            if_not_exists: Si True, no lanza error si ya existe.
            properties:    Propiedades extra (OPTIONS).
        """
        if name in self._catalogs:
            if if_not_exists:
                return None
            raise ValueError(f"El catálogo '{name}' ya existe.")

        # MANAGED LOCATION: ruta personalizada o default warehouse
        wh = location or os.path.join(_warehouse_base(), name)
        # Solo crear directorio local; las rutas S3/remota las gestiona Spark
        if "://" not in wh:
            pathlib.Path(wh).mkdir(parents=True, exist_ok=True)
            for sch in ("default", "bronze", "silver", "gold"):
                pathlib.Path(os.path.join(wh, sch)).mkdir(parents=True, exist_ok=True)
        self._catalogs[name] = wh

        # Registrar en la sesión Spark activa solo si el catálogo no es uno de
        # los catalogs core ya inicializados.  Registrar catálogos dinámicos
        # via spark.conf.set() en una sesión activa de Spark 3.5 + DeltaCatalog
        # provoca un bug conocido (SPARK-47789) donde el optimizador lanza
        # NullPointerException en operaciones posteriores.
        # Los catálogos personalizados son gestionados íntegramente por el shim UC.
        _core_catalogs = {"main", "hive_metastore", "spark_catalog"}
        if name in _core_catalogs:
            self._spark.conf.set(
                f"spark.sql.catalog.{name}",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            self._spark.conf.set(f"spark.sql.catalog.{name}.warehouse", wh)

        print(
            f"[Unity] Catálogo '{name}' creado → {wh}"
            + (f" (MANAGED LOCATION: {location})" if location else "")
        )
        return None

    def drop_catalog(
        self, name: str, if_exists: bool = False, cascade: bool = False
    ) -> None:
        """Elimina un catálogo."""
        _protected = {"main", "hive_metastore", "spark_catalog", "system"}
        if name in _protected:
            raise ValueError(f"No se puede eliminar el catálogo protegido '{name}'.")
        if name not in self._catalogs:
            if if_exists:
                return None
            raise ValueError(f"El catálogo '{name}' no existe.")
        if cascade:
            shutil.rmtree(self._catalogs[name], ignore_errors=True)
        del self._catalogs[name]
        print(f"[Unity] Catálogo '{name}' eliminado.")
        return None

    def list_catalogs(self) -> List[CatalogInfo]:
        """Lista todos los catálogos conocidos."""
        return [CatalogInfo(name=n, comment="") for n in sorted(self._catalogs)]

    def get_current_catalog(self) -> str:
        """Devuelve el catálogo activo."""
        global _CURRENT_CATALOG
        return _CURRENT_CATALOG

    def set_current_catalog(self, name: str) -> None:
        """Cambia el catálogo activo."""
        global _CURRENT_CATALOG
        _CURRENT_CATALOG = name
        try:
            self._spark.sql(f"USE CATALOG `{name}`")
        except Exception:
            pass  # catálogos añadidos dinámicamente pueden no estar en Spark

    def set_current_schema(self, name: str) -> None:
        """Cambia el schema activo."""
        global _CURRENT_SCHEMA
        _CURRENT_SCHEMA = name
        try:
            self._spark.sql(f"USE DATABASE `{name}`")
        except Exception:
            pass

    def get_current_schema(self) -> str:
        """Devuelve el schema activo."""
        return _CURRENT_SCHEMA

    # ── Gestión de schemas ────────────────────────────────────────────────────

    def create_schema(
        self,
        catalog_name: str,
        schema_name: str,
        comment: str = "",
        if_not_exists: bool = False,
        properties: Optional[Dict] = None,
    ) -> None:
        ine = "IF NOT EXISTS" if if_not_exists else ""
        comment_clause = f"COMMENT '{comment}'" if comment else ""
        prop_clause = ""
        if properties:
            kv = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
            prop_clause = f"WITH DBPROPERTIES ({kv})"

        # El three-level namespace (catalog.schema) produce NullPointerException
        # con DeltaCatalog en Spark 3.5.x (SPARK-47789).  Usamos la variante
        # 2-level asegurándonos de que el catálogo activo sea el correcto.
        try:
            default_cat = self._spark.conf.get("spark.sql.defaultCatalog", "main")
        except Exception:
            default_cat = "main"

        if catalog_name == default_cat:
            # Sin prefijo de catálogo → usa el catálogo activo (main)
            self._spark.sql(
                f"CREATE DATABASE {ine} `{schema_name}` "
                f"{comment_clause} {prop_clause}".strip()
            )
        else:
            # Intentar 3-level; si falla (catálogo custom sin DeltaCatalog),
            # registrar solo en el registro en memoria.
            try:
                self._spark.sql(
                    f"CREATE DATABASE {ine} `{catalog_name}`.`{schema_name}` "
                    f"{comment_clause} {prop_clause}".strip()
                )
            except Exception:
                # Catálogo custom gestionado solo por el shim
                pass

    def drop_schema(
        self,
        catalog_name: str,
        schema_name: str,
        if_exists: bool = False,
        cascade: bool = False,
    ) -> None:
        ie = "IF EXISTS" if if_exists else ""
        casc = "CASCADE" if cascade else ""
        try:
            default_cat = self._spark.conf.get("spark.sql.defaultCatalog", "main")
        except Exception:
            default_cat = "main"

        if catalog_name == default_cat:
            self._spark.sql(f"DROP DATABASE {ie} `{schema_name}` {casc}".strip())
        else:
            try:
                self._spark.sql(
                    f"DROP DATABASE {ie} `{catalog_name}`.`{schema_name}` {casc}".strip()
                )
            except Exception:
                pass

    def list_schemas(self, catalog_name: Optional[str] = None) -> List[SchemaInfo]:
        cat = catalog_name or _CURRENT_CATALOG
        try:
            default_cat = self._spark.conf.get("spark.sql.defaultCatalog", "main")
        except Exception:
            default_cat = "main"

        try:
            if cat == default_cat:
                rows = self._spark.sql("SHOW SCHEMAS").collect()
            else:
                rows = self._spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect()
            return [SchemaInfo(catalog_name=cat, name=r[0], comment="") for r in rows]
        except Exception:
            return []

    def describe_schema(self, catalog_name: str, schema_name: str):
        """Describe un schema, devuelve DataFrame con la información."""
        try:
            default_cat = self._spark.conf.get("spark.sql.defaultCatalog", "main")
        except Exception:
            default_cat = "main"
        try:
            if catalog_name == default_cat:
                return self._spark.sql(f"DESCRIBE DATABASE `{schema_name}`")
            else:
                return self._spark.sql(
                    f"DESCRIBE DATABASE `{catalog_name}`.`{schema_name}`"
                )
        except Exception:
            rows = [
                ("Database Name", schema_name),
                ("Catalog Name", catalog_name),
                ("Comment", ""),
                ("Location", self._catalogs.get(catalog_name, "unknown")),
            ]
            return self._spark.createDataFrame(
                rows, "info_name STRING, info_value STRING"
            )

    # ── Gestión de tablas ─────────────────────────────────────────────────────

    def list_tables(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ) -> List[TableInfo]:
        cat = catalog_name or _CURRENT_CATALOG
        sch = schema_name or "default"
        try:
            default_cat = self._spark.conf.get("spark.sql.defaultCatalog", "main")
        except Exception:
            default_cat = "main"

        try:
            if cat == default_cat:
                rows = self._spark.sql(f"SHOW TABLES IN `{sch}`").collect()
            else:
                rows = self._spark.sql(f"SHOW TABLES IN `{cat}`.`{sch}`").collect()
            return [
                TableInfo(
                    catalog_name=cat, schema_name=sch, name=r[1], table_type="MANAGED"
                )
                for r in rows
            ]
        except Exception:
            return []

    def describe_table(self, full_table_name: str):
        """Describe columnas de una tabla (catalog.schema.table)."""
        parts = full_table_name.replace("`", "").split(".")
        ref = ".".join(f"`{p}`" for p in parts)
        return self._spark.sql(f"DESCRIBE TABLE {ref}")

    def list_columns(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> List[ColumnInfo]:
        """Lista las columnas de una tabla."""
        df = self.describe_table(f"{catalog_name}.{schema_name}.{table_name}")
        return [
            ColumnInfo(
                name=r["col_name"],
                data_type=r["data_type"],
                nullable=True,
                comment=r.get("comment", ""),
            )
            for r in df.collect()
            if r["col_name"] and not r["col_name"].startswith("#")
        ]

    # ── Gestión de Volumes ────────────────────────────────────────────────────

    def create_volume(
        self,
        catalog_name: str,
        schema_name: str,
        volume_name: str,
        volume_type: str = "MANAGED",
        location: Optional[str] = None,
        comment: str = "",
        if_not_exists: bool = False,
    ) -> None:
        """Crea un volume (directorio local mapeado a /Volumes/)."""
        key = f"{catalog_name}.{schema_name}.{volume_name}"
        if key in self._volumes:
            if if_not_exists:
                return None
            raise FileExistsError(f"El volume '{key}' ya existe.")

        local_path = location or _join(
            _volumes_root(),
            f"{catalog_name}/{schema_name}/{volume_name}",
        )
        # Solo crear directorio para rutas locales; S3/remota las gestiona Spark
        if "://" not in local_path:
            pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)
        self._volumes[key] = local_path
        print(f"[Unity] Volume '{key}' ({volume_type}) → {local_path}")
        return None

    def drop_volume(
        self,
        catalog_name: str,
        schema_name: str,
        volume_name: str,
        if_exists: bool = False,
    ) -> None:
        key = f"{catalog_name}.{schema_name}.{volume_name}"
        if key not in self._volumes:
            if if_exists:
                return None
            raise FileNotFoundError(f"Volume '{key}' no encontrado.")
        shutil.rmtree(self._volumes[key], ignore_errors=True)
        del self._volumes[key]
        print(f"[Unity] Volume '{key}' eliminado.")
        return None

    def list_volumes(
        self, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> List[VolumeInfo]:
        """Lista volumes, combinando registro en memoria y escaneo de disco."""
        cat = catalog or _CURRENT_CATALOG
        results: Dict[str, VolumeInfo] = {}

        # Desde el registro en memoria
        for key, path in self._volumes.items():
            parts = key.split(".")
            if len(parts) != 3:
                continue
            c, s, v = parts
            if c != cat or (schema and s != schema):
                continue
            results[key] = VolumeInfo(
                catalog_name=c,
                schema_name=s,
                name=v,
                volume_type="MANAGED",
                storage_location=path,
            )

        # Escaneo del sistema de archivos local
        vol_root = pathlib.Path(os.path.join(_volumes_root(), cat))
        if vol_root.exists():
            for sch_dir in vol_root.iterdir():
                if not sch_dir.is_dir():
                    continue
                if schema and sch_dir.name != schema:
                    continue
                for vol_dir in sch_dir.iterdir():
                    if not vol_dir.is_dir():
                        continue
                    key = f"{cat}.{sch_dir.name}.{vol_dir.name}"
                    if key not in results:
                        results[key] = VolumeInfo(
                            catalog_name=cat,
                            schema_name=sch_dir.name,
                            name=vol_dir.name,
                            volume_type="MANAGED",
                            storage_location=str(vol_dir),
                        )

        return list(results.values())

    def volume_path(
        self, catalog_name: str, schema_name: str, volume_name: str, *subpath: str
    ) -> str:
        """
        Devuelve la ruta local de un volume, opcionalmente con subpath.

        Equivale a ``/Volumes/catalog/schema/volume/subpath`` resuelto a local.
        """
        base = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
        if subpath:
            base = base + "/" + "/".join(subpath)
        return resolve_volume_path(base)

    # ── Tags ──────────────────────────────────────────────────────────────────

    def set_tags(self, object_ref: str, tags: Dict[str, str]) -> None:
        """Asigna tags a cualquier objeto UC (tabla, schema, catálogo…)."""
        self._tags.setdefault(object_ref, {}).update(tags)

    def get_tags(self, object_ref: str) -> Dict[str, str]:
        """Devuelve los tags de un objeto."""
        return dict(self._tags.get(object_ref, {}))

    def unset_tags(self, object_ref: str, tag_keys: List[str]) -> None:
        """Elimina tags específicos de un objeto."""
        store = self._tags.get(object_ref, {})
        for k in tag_keys:
            store.pop(k, None)

    # ── Grants / Permisos ─────────────────────────────────────────────────────

    def grant(
        self, privilege: str, object_type: str, object_ref: str, principal: str
    ) -> None:
        """Grant de un privilegio (no-op, registrado en audit log)."""
        entry = {
            "principal": principal,
            "privilege": privilege,
            "object_type": object_type,
            "object_key": object_ref,
            "granted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        self._grants.append(entry)
        print(
            f"[Unity] GRANT {privilege} ON {object_type} "
            f"{object_ref} TO {principal}"
        )

    def revoke(
        self, privilege: str, object_type: str, object_ref: str, principal: str
    ) -> None:
        """Revoke de un privilegio (no-op, registrado en audit log)."""
        self._grants[:] = [
            g
            for g in self._grants
            if not (
                g["principal"] == principal
                and g["privilege"] == privilege
                and g["object_key"] == object_ref
            )
        ]
        print(
            f"[Unity] REVOKE {privilege} ON {object_type} "
            f"{object_ref} FROM {principal}"
        )

    def show_grants(
        self,
        principal: Optional[str] = None,
        object_type: Optional[str] = None,
        object_ref: Optional[str] = None,
    ) -> List[GrantInfo]:
        """Lista grants del audit log local, con filtros opcionales."""
        results = list(self._grants)
        if principal:
            results = [g for g in results if g["principal"] == principal]
        if object_ref:
            results = [g for g in results if g["object_key"] == object_ref]
        return [
            GrantInfo(
                principal=g["principal"],
                privilege=g["privilege"],
                object_type=g["object_type"],
                object_key=g["object_key"],
            )
            for g in results
        ]

    def audit_log(self):
        """DataFrame con el log de auditoría de Unity Catalog (grants/revokes)."""
        rows = [
            (
                g["principal"],
                g["privilege"],
                g["object_type"],
                g["object_key"],
                g.get("granted_at", ""),
            )
            for g in self._grants
        ]
        schema_str = (
            "user_identity STRING, action_name STRING, "
            "request_object_type STRING, request_object_name STRING, "
            "event_time STRING"
        )
        return (
            self._spark.createDataFrame(rows, schema_str)
            if rows
            else self._spark.createDataFrame([], schema_str)
        )

    # ── Row filters / Column masks (no-op) ───────────────────────────────────

    def create_row_filter(
        self, catalog_name: str, schema_name: str, function_name: str, **kwargs
    ) -> None:
        print(f"[Unity] create_row_filter '{function_name}' (no-op locally)")

    def create_column_mask(
        self, catalog_name: str, schema_name: str, function_name: str, **kwargs
    ) -> None:
        print(f"[Unity] create_column_mask '{function_name}' (no-op locally)")

    # ── External locations / Storage credentials (no-op) ─────────────────────

    def create_storage_credential(self, name: str, **kwargs) -> None:
        print(f"[Unity] create_storage_credential '{name}' (no-op locally)")

    def create_external_location(
        self, name: str, url: str, credential: str, **kwargs
    ) -> None:
        print(f"[Unity] create_external_location '{name}' → {url} (no-op locally)")

    def list_storage_credentials(self) -> List:
        return []

    def list_external_locations(self) -> List:
        return []

    # ── Delta Sharing (no-op) ─────────────────────────────────────────────────

    def create_share(self, name: str, comment: str = "") -> None:
        print(f"[Unity] create_share '{name}' (no-op locally)")

    def create_recipient(self, name: str, **kwargs) -> None:
        print(f"[Unity] create_recipient '{name}' (no-op locally)")

    def list_shares(self) -> List:
        return []

    def list_recipients(self) -> List:
        return []

    # ── Gestión de funciones ──────────────────────────────────────────────────

    def create_function(
        self,
        catalog_name: str,
        schema_name: str,
        function_name: str,
        definition: str = "",
        description: str = "",
        if_not_exists: bool = False,
    ) -> None:
        """Registra una función en el catálogo UC (shim en memoria)."""
        key = f"{catalog_name}.{schema_name}.{function_name}"
        if key in _FUNCTION_REGISTRY:
            if if_not_exists:
                return None
            raise ValueError(f"La función '{key}' ya existe.")
        _FUNCTION_REGISTRY[key] = {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "name": function_name,
            "definition": definition,
            "description": description,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        print(f"[Unity] Función '{key}' registrada.")
        return None

    def drop_function(
        self,
        catalog_name: str,
        schema_name: str,
        function_name: str,
        if_exists: bool = False,
    ) -> None:
        """Elimina una función del registro UC."""
        key = f"{catalog_name}.{schema_name}.{function_name}"
        if key not in _FUNCTION_REGISTRY:
            if if_exists:
                return None
            raise ValueError(f"La función '{key}' no existe.")
        del _FUNCTION_REGISTRY[key]
        print(f"[Unity] Función '{key}' eliminada.")
        return None

    def list_functions(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ) -> List[FunctionInfo]:
        """Lista funciones registradas en UC."""
        cat = catalog_name or _CURRENT_CATALOG
        sch = schema_name or "default"
        results = []
        for key, info in _FUNCTION_REGISTRY.items():
            if info["catalog_name"] == cat and (
                schema_name is None or info["schema_name"] == sch
            ):
                results.append(
                    FunctionInfo(
                        catalog_name=info["catalog_name"],
                        schema_name=info["schema_name"],
                        name=info["name"],
                        description=info.get("description", ""),
                    )
                )
        return results

    def describe_function_sql(self, ref: str):
        """Describe una función, devuelve DataFrame."""
        parts = ref.replace("`", "").split(".")
        if len(parts) == 3:
            key = ref.replace("`", "")
        elif len(parts) == 2:
            key = f"{_CURRENT_CATALOG}.{parts[0]}.{parts[1]}"
        else:
            key = f"{_CURRENT_CATALOG}.default.{parts[0]}"
        info = _FUNCTION_REGISTRY.get(key, {})
        rows = [
            ("Function", key),
            ("Type", "SCALAR"),
            ("Description", info.get("description", "")),
            ("Definition", info.get("definition", "")),
        ]
        return self._spark.createDataFrame(rows, "info_name STRING, info_value STRING")

    # ── Gestión de grupos ─────────────────────────────────────────────────────

    def create_group(self, name: str, if_not_exists: bool = False) -> None:
        """Crea un grupo de seguridad (en memoria)."""
        if name in _GROUP_REGISTRY:
            if if_not_exists:
                return None
            raise ValueError(f"El grupo '{name}' ya existe.")
        _GROUP_REGISTRY[name] = []
        print(f"[Unity] Grupo '{name}' creado.")
        return None

    def drop_group(self, name: str, if_exists: bool = False) -> None:
        """Elimina un grupo de seguridad."""
        if name not in _GROUP_REGISTRY:
            if if_exists:
                return None
            raise ValueError(f"El grupo '{name}' no existe.")
        del _GROUP_REGISTRY[name]
        print(f"[Unity] Grupo '{name}' eliminado.")
        return None

    def add_group_member(self, group_name: str, member: str) -> None:
        """Añade un miembro a un grupo."""
        if group_name not in _GROUP_REGISTRY:
            _GROUP_REGISTRY[group_name] = []
        if member not in _GROUP_REGISTRY[group_name]:
            _GROUP_REGISTRY[group_name].append(member)
        print(f"[Unity] '{member}' añadido al grupo '{group_name}'.")
        return None

    def remove_group_member(self, group_name: str, member: str) -> None:
        """Elimina un miembro de un grupo."""
        if group_name in _GROUP_REGISTRY:
            _GROUP_REGISTRY[group_name] = [
                m for m in _GROUP_REGISTRY[group_name] if m != member
            ]
        print(f"[Unity] '{member}' eliminado del grupo '{group_name}'.")
        return None

    def list_groups(self) -> List[GroupInfo]:
        """Lista todos los grupos."""
        return [
            GroupInfo(name=n, members=list(m))
            for n, m in sorted(_GROUP_REGISTRY.items())
        ]

    # ── DENY (complemento de GRANT/REVOKE) ────────────────────────────────────

    def deny(
        self, privilege: str, object_type: str, object_ref: str, principal: str
    ) -> None:
        """Deny — niega un privilegio (registrado en audit log)."""
        entry = {
            "principal": principal,
            "privilege": f"DENY:{privilege}",
            "object_type": object_type,
            "object_key": object_ref,
            "granted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        self._grants.append(entry)
        print(
            f"[Unity] DENY {privilege} ON {object_type} " f"{object_ref} TO {principal}"
        )

    # ── UNDROP TABLE / SHOW TABLES DROPPED ────────────────────────────────────

    def track_drop_table(self, table_ref: str) -> None:
        """Registra una tabla eliminada para soporte de UNDROP."""
        parts = table_ref.replace("`", "").split(".")
        if len(parts) == 3:
            cat, sch, tbl = parts
        elif len(parts) == 2:
            cat, sch, tbl = _CURRENT_CATALOG, parts[0], parts[1]
        else:
            cat, sch, tbl = _CURRENT_CATALOG, _CURRENT_SCHEMA, parts[0]
        _DROPPED_TABLES.append(
            {
                "catalog": cat,
                "schema": sch,
                "table": tbl,
                "dropped_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
        )

    def undrop_table(self, table_ref: str) -> None:
        """UNDROP TABLE — restaura una tabla previamente eliminada."""
        parts = table_ref.replace("`", "").split(".")
        if len(parts) == 3:
            tbl = parts[2]
        elif len(parts) == 2:
            tbl = parts[1]
        else:
            tbl = parts[0]
        found = None
        for i, d in enumerate(_DROPPED_TABLES):
            if d["table"] == tbl:
                found = i
                break
        if found is not None:
            _DROPPED_TABLES.pop(found)
            print(f"[Unity] UNDROP TABLE '{table_ref}' — restaurada.")
        else:
            print(
                f"[Unity] UNDROP TABLE '{table_ref}' — no encontrada en dropped tables."
            )
        return None

    def list_dropped_tables(self) -> List[DroppedTableInfo]:
        """Lista tablas eliminadas (SHOW TABLES DROPPED)."""
        return [
            DroppedTableInfo(
                catalog_name=d["catalog"],
                schema_name=d["schema"],
                name=d["table"],
                dropped_at=d["dropped_at"],
            )
            for d in _DROPPED_TABLES
        ]

    # ── Lineage (tracking básico) ─────────────────────────────────────────────

    def track_lineage(
        self, source: str, target: str, lineage_type: str = "TABLE"
    ) -> None:
        """Registra una relación de linaje entre dos objetos."""
        _LINEAGE_LOG.append(
            {
                "source": source,
                "target": target,
                "lineage_type": lineage_type,
                "tracked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
        )

    def get_lineage(self, object_ref: Optional[str] = None) -> List[LineageInfo]:
        """Devuelve el linaje registrado, opcionalmente filtrado."""
        results = _LINEAGE_LOG
        if object_ref:
            results = [
                l
                for l in results
                if l["source"] == object_ref or l["target"] == object_ref
            ]
        return [
            LineageInfo(
                source=l["source"], target=l["target"], lineage_type=l["lineage_type"]
            )
            for l in results
        ]

    def lineage_as_dataframe(self):
        """Devuelve el linaje como DataFrame (simula system.lineage)."""
        rows = [
            (l["source"], l["target"], l["lineage_type"], l.get("tracked_at", ""))
            for l in _LINEAGE_LOG
        ]
        schema_str = (
            "source_table STRING, target_table STRING, "
            "lineage_type STRING, tracked_at STRING"
        )
        return (
            self._spark.createDataFrame(rows, schema_str)
            if rows
            else self._spark.createDataFrame([], schema_str)
        )

    # ── system.information_schema (básico) ───────────────────────────────────

    @property
    def information_schema(self) -> "_InformationSchema":
        return _InformationSchema(self._spark, self)

    # ── Utilidades ────────────────────────────────────────────────────────────

    def help(self) -> None:
        print("""
Unity Catalog Shim — métodos disponibles   (docs.databricks.com Feb 2026)
──────────────────────────────────────────────────────────────────────────
SQL interceptado (no soportado nativamente por Spark):

  Catálogos:
    CREATE CATALOG [IF NOT EXISTS] name
        [MANAGED LOCATION 'path'] [COMMENT 'text'] [DEFAULT COLLATION name]
    DROP CATALOG [IF EXISTS] name [CASCADE]
    DESCRIBE CATALOG [EXTENDED] name
    ALTER CATALOG name SET OWNER TO principal
    ALTER CATALOG name SET LOCATION 'path'
    CREATE FOREIGN CATALOG ...   (no-op — requiere Lakehouse Federation)
    SHOW CATALOGS [LIKE 'pattern']
    USE CATALOG name

  Schemas/Databases:
    CREATE SCHEMA/DATABASE [IF NOT EXISTS] [catalog.]schema [COMMENT '...']
    DROP SCHEMA/DATABASE [IF EXISTS] [catalog.]schema [CASCADE]
    DESCRIBE SCHEMA/DATABASE [EXTENDED] [catalog.]schema
    SHOW SCHEMAS/DATABASES [IN catalog] [LIKE 'pattern']
    USE SCHEMA/DATABASE [catalog.]schema

  Tablas:
    SHOW TABLES [IN catalog.schema] [LIKE 'pattern']
    SHOW VIEWS [IN catalog.schema] [LIKE 'pattern']
    SHOW COLUMNS IN table
    SHOW CREATE TABLE table
    UNDROP TABLE name
    SHOW TABLES DROPPED [IN schema]

  Volumes:
    CREATE [EXTERNAL] VOLUME [IF NOT EXISTS] cat.sch.vol
        [LOCATION 'path'] [COMMENT 'text']
    DROP VOLUME [IF EXISTS] cat.sch.vol
    DESCRIBE VOLUME [EXTENDED] cat.sch.vol
    SHOW VOLUMES [IN cat[.sch]] [LIKE 'pattern']
    ALTER VOLUME ref SET OWNER TO / RENAME TO

  Funciones:
    CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name ...
    DROP FUNCTION [IF EXISTS] name
    DESCRIBE FUNCTION [EXTENDED] name
    SHOW FUNCTIONS [IN catalog.schema] [LIKE 'pattern']

  Permisos:
    GRANT privilege ON securable_type [ref] TO principal
    REVOKE privilege ON securable_type [ref] FROM principal
    DENY privilege ON securable_type [ref] TO principal
    SHOW GRANTS [ON type ref] [TO principal]
    GRANT ON SHARE share TO recipient
    REVOKE ON SHARE share FROM recipient
    SHOW GRANTS ON SHARE share
    SHOW GRANTS TO RECIPIENT recipient
    Securable types: METASTORE, TABLE, VIEW, MATERIALIZED VIEW,
      SCHEMA, CATALOG, VOLUME, FUNCTION, MODEL, PROCEDURE, CONNECTION,
      CLEAN ROOM, SERVICE CREDENTIAL, STORAGE CREDENTIAL, SHARE, etc.

  Grupos:
    CREATE GROUP [IF NOT EXISTS] name
    DROP GROUP [IF EXISTS] name
    ALTER GROUP name ADD/REMOVE USER member
    SHOW GROUPS
    SHOW USERS

  Tags:
    ALTER TABLE|VIEW|CATALOG|SCHEMA|VOLUME ref SET TAGS ('k'='v', ...)
    ALTER TABLE|VIEW|CATALOG|SCHEMA|VOLUME ref UNSET TAGS ('k', ...)
    SHOW TAGS ON [type] ref

  Otros:
    COMMENT ON TABLE|VIEW|VOLUME|CATALOG|SCHEMA ref IS 'text'
    ALTER TABLE|SCHEMA|VOLUME ref SET OWNER TO principal
    LIST 'path'
    GET 'src' TO 'dst' / PUT 'src' INTO 'dst' / REMOVE 'path'

  No-ops: CREATE/DROP/ALTER/DESCRIBE/SHOW SHARE, RECIPIENT, PROVIDER,
    EXTERNAL LOCATION, STORAGE CREDENTIAL, SERVICE CREDENTIAL,
    CONNECTION, CLEAN ROOM, MATERIALIZED VIEW, STREAMING TABLE,
    PROCEDURE, SERVER, REFRESH FOREIGN/MATERIALIZED/STREAMING,
    SYNC, MSCK REPAIR PRIVILEGES, SET RECIPIENT, ...

API Python:
  Catálogos : create_catalog, drop_catalog, list_catalogs,
              get/set_current_catalog
  Schemas   : create_schema, drop_schema, list_schemas, describe_schema,
              get/set_current_schema
  Tablas    : list_tables, describe_table, list_columns,
              track_drop_table, undrop_table, list_dropped_tables
  Volumes   : create_volume, drop_volume, list_volumes, volume_path
  Funciones : create_function, drop_function, list_functions,
              describe_function_sql
  Tags      : set_tags, get_tags, unset_tags
  Grants    : grant, revoke, deny, show_grants, audit_log
  Grupos    : create_group, drop_group, add/remove_group_member, list_groups
  Linaje    : track_lineage, get_lineage, lineage_as_dataframe
  Seguridad : create_row_filter, create_column_mask  (no-op)
  Almacen.  : create_storage_credential, create_external_location  (no-op)
  Sharing   : create_share, create_recipient  (no-op)
  Schema UC : information_schema.catalogs/schemata/tables/columns/
              volumes/table_privileges/routines()

Rutas especiales (via dbutils.fs):
  /Volumes/catalog/schema/volume/path  →  <VOLUMES_ROOT>/…
  dbfs:/path                           →  <DBFS_ROOT>/…
  s3a://bucket/…                       →  Hadoop FileSystem (MinIO/S3)
""")


# ── information_schema simulado ───────────────────────────────────────────────


class _InformationSchema:
    """Subconjunto de system.information_schema de Unity Catalog."""

    def __init__(self, spark, uc: UnityCatalogShim):
        self._spark = spark
        self._uc = uc

    def catalogs(self):
        rows = [(c.name, c.comment) for c in self._uc.list_catalogs()]
        return self._spark.createDataFrame(rows, "catalog_name STRING, comment STRING")

    def schemata(self, catalog_name: Optional[str] = None):
        schemas = self._uc.list_schemas(catalog_name)
        rows = [(s.catalog_name, s.name, s.comment) for s in schemas]
        return self._spark.createDataFrame(
            rows, "catalog_name STRING, schema_name STRING, comment STRING"
        )

    def tables(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ):
        tables = self._uc.list_tables(catalog_name, schema_name)
        rows = [(t.catalog_name, t.schema_name, t.name, t.table_type) for t in tables]
        return self._spark.createDataFrame(
            rows,
            "table_catalog STRING, table_schema STRING, "
            "table_name STRING, table_type STRING",
        )

    def columns(
        self,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ):
        """Lista columnas — simula system.information_schema.columns."""
        cat = catalog_name or _CURRENT_CATALOG
        sch = schema_name or "default"
        if table_name:
            try:
                cols = self._uc.list_columns(cat, sch, table_name)
                rows = [
                    (cat, sch, table_name, c.name, c.data_type, c.nullable, c.comment)
                    for c in cols
                ]
            except Exception:
                rows = []
        else:
            rows = []
        schema_str = (
            "table_catalog STRING, table_schema STRING, "
            "table_name STRING, column_name STRING, "
            "data_type STRING, is_nullable BOOLEAN, comment STRING"
        )
        return (
            self._spark.createDataFrame(rows, schema_str)
            if rows
            else self._spark.createDataFrame([], schema_str)
        )

    def volumes(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ):
        vols = self._uc.list_volumes(catalog_name, schema_name)
        rows = [
            (v.catalog_name, v.schema_name, v.name, v.volume_type, v.storage_location)
            for v in vols
        ]
        return self._spark.createDataFrame(
            rows,
            "volume_catalog STRING, volume_schema STRING, volume_name STRING, "
            "volume_type STRING, storage_location STRING",
        )

    def table_privileges(self, catalog_name: Optional[str] = None):
        """Lista privilegios — simula system.information_schema.table_privileges."""
        grants = self._uc.show_grants()
        if catalog_name:
            grants = [g for g in grants if g.object_key.startswith(catalog_name + ".")]
        rows = [(g.principal, g.privilege, g.object_type, g.object_key) for g in grants]
        schema_str = (
            "grantee STRING, privilege_type STRING, "
            "object_type STRING, object_name STRING"
        )
        return (
            self._spark.createDataFrame(rows, schema_str)
            if rows
            else self._spark.createDataFrame([], schema_str)
        )

    def routines(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ):
        """Lista funciones — simula system.information_schema.routines."""
        funcs = self._uc.list_functions(catalog_name, schema_name)
        rows = [(f.catalog_name, f.schema_name, f.name, f.description) for f in funcs]
        schema_str = (
            "routine_catalog STRING, routine_schema STRING, "
            "routine_name STRING, routine_definition STRING"
        )
        return (
            self._spark.createDataFrame(rows, schema_str)
            if rows
            else self._spark.createDataFrame([], schema_str)
        )


# ── Inicialización de catálogos por defecto ───────────────────────────────────


def init_unity_catalog(spark, warehouse_s3_base: Optional[str] = None) -> None:
    """
    Inicializa la estructura de Unity Catalog local.
    Llamada automáticamente por ``get_spark_session()``.

    Registra ``main`` y ``hive_metastore`` en el registro global
    y crea los directorios de warehouse y volumes.
    """
    global _CATALOG_REGISTRY, _CURRENT_CATALOG, _CURRENT_SCHEMA

    base = warehouse_s3_base or _warehouse_base()

    for cat in ("main", "hive_metastore"):
        if warehouse_s3_base:
            cat_path = _join(warehouse_s3_base, cat)
        else:
            cat_path = os.path.join(base, cat)
            pathlib.Path(cat_path).mkdir(parents=True, exist_ok=True)
        _CATALOG_REGISTRY[cat] = cat_path

    # Directorios locales para volumes y DBFS
    # (omitir si apuntan a rutas remotas como s3a://)
    vr = _volumes_root()
    if "://" not in vr:
        pathlib.Path(vr).mkdir(parents=True, exist_ok=True)
    dr = _dbfs_root()
    if "://" not in dr:
        pathlib.Path(dr).mkdir(parents=True, exist_ok=True)

    _CURRENT_CATALOG = "main"


# ── Helpers internos ──────────────────────────────────────────────────────────


def _parse_tags(tags_str: str) -> Dict[str, str]:
    """Parsea 'key' = 'value', 'key2' = 'value2' a dict."""
    result = {}
    for m in re.finditer(r"['\"`]?(\w+)['\"`]?\s*=\s*['\"]([^'\"]*)['\"]", tags_str):
        result[m.group(1)] = m.group(2)
    return result
