# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-02-18

### Removed
- **Docker infrastructure**: Removed `Dockerfile` and `docker-compose.yml`
- **MinIO (S3)**: Removed all S3A/MinIO configuration and Hadoop FS remote code paths
- **PostgreSQL**: Removed Hive Metastore PostgreSQL backend
- **boto3 dependency**: No longer needed without S3 support
- **mlflow dependency**: Removed (was unused)
- **APP_ENV branching**: Removed local/docker/cloud mode switching
- **Hadoop FS helpers**: Removed `_is_remote()`, `_hadoop_fs()` from `utils.py`

### Changed
- Project now runs **100% locally** — no Docker, no external services
- `connect.py`: Simplified to ~90 lines (was ~196). Pure local Spark + Delta Lake
- `utils.py`: All FSMock methods now use only local filesystem operations
- `unity_catalog.py`: Removed S3 path conditionals from `init_unity_catalog`, `create_catalog`, `create_volume`
- `main.py`: Uses local filesystem paths instead of `s3a://` URLs
- `.env.example`: Simplified to local-only configuration
- Updated README, NOTICE, CONTRIBUTING to reflect local-only architecture

## [0.4.0] - 2026-02-18

### Added
- **Unity Catalog Complete Implementation** (~20+ feature categories)
  - USE CATALOG / USE SCHEMA support with state management
  - SHOW CATALOGS, SHOW SCHEMAS with filtering
  - CREATE/DROP/DESCRIBE SCHEMA (SQL + Python API)
  - User-defined functions: CREATE/DROP/DESCRIBE/SHOW FUNCTION
  - Groups: CREATE/DROP/ALTER GROUP, SHOW GROUPS, ADD/REMOVE USER
  - DENY command for permission denial
  - UNDROP TABLE / SHOW TABLES DROPPED for table recovery
  - Data lineage tracking with `track_lineage()` and `get_lineage()`
  - GRANT/REVOKE ON SHARE, SHOW GRANTS ON SHARE/TO RECIPIENT
  - ALTER VOLUME (SET OWNER, RENAME)
  - File operations: GET FILE, PUT FILE, REMOVE FILE, LIST
  - Expanded information_schema: columns(), table_privileges(), routines()
  - 40+ additional no-op patterns for cloud infrastructure commands
    - ALTER/DESCRIBE for STORAGE CREDENTIAL, SERVICE CREDENTIAL
    - CREATE/DROP/DESCRIBE/SHOW CLEAN ROOM
    - CREATE/DROP/SHOW SERVER
    - REFRESH MATERIALIZED VIEW, REFRESH STREAMING TABLE
    - SYNC SCHEMA, MSCK REPAIR PRIVILEGES
    - SET RECIPIENT PROPERTIES

### Changed
- Expanded `_NOOP_PATTERNS` from ~25 to ~65+ entries
- Updated test fixture to clear new registries (functions, groups, lineage, dropped tables)
- Improved pattern ordering in SQL interceptor to prevent conflicts

### Fixed
- `_PAT_DENY` regex double-escaped backslashes
- Pattern ordering issues (DENY/UNDROP/SHOW GRANTS TO RECIPIENT now before generals)
- Missing no-op patterns causing failures

### Tests
- Added 117 new tests (total: 253 passed, 3 xfailed)
- Test classes for all new features
- Comprehensive coverage for SQL and Python APIs

## [0.3.0] - 2026-02-17

### Added
- Unity Catalog core features
  - Catalog management (CREATE, DROP, DESCRIBE)
  - Schema management (3-level namespace)
  - Volume support with `/Volumes/` paths
  - GRANT/REVOKE/SHOW GRANTS for permissions
  - SET/UNSET TAGS, COMMENT ON support
  - Audit logging with `audit_log()`
  - information_schema views (catalogs, schemata, tables, volumes)

- DBUtils complete implementation
  - `dbutils.credentials` (no-op locally)
  - `dbutils.secrets` with scope management
  - `dbutils.widgets` (text, dropdown, combobox, multiselect)
  - `dbutils.fs` with `/Volumes/` and `dbfs:/` support
  - `dbutils.notebook.run()` and `exit()`
  - `dbutils.jobs.taskValues`
  - `dbutils.data.summarize()`

### Changed
- Refactored SparkSession creation to detect Docker vs local mode
- Improved `display()` function with better DataFrame rendering

## [0.2.0] - 2026-02-15

### Added
- Docker Compose environment with MinIO and PostgreSQL
- Medallion architecture ETL demo (`main.py`)
- Delta Lake time travel support
- Example notebook (`notebooks/analysis.ipynb`)

### Changed
- Switched to PySpark 3.5.3 and Delta Lake 3.3.2
- Updated to emulate Databricks Runtime 16.4 LTS

## [0.1.0] - 2026-02-10

### Added
- Initial release
- Basic PySpark integration
- Delta Lake support
- Simple Unity Catalog shim
- Test suite with pytest

---

## Types of changes
- `Added` for new features
- `Changed` for changes in existing functionality
- `Deprecated` for soon-to-be removed features
- `Removed` for now removed features
- `Fixed` for any bug fixes
- `Security` in case of vulnerabilities
