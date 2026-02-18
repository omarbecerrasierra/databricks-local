# PySpark Unity Catalog Local Development Environment

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PySpark 3.5.3](https://img.shields.io/badge/pyspark-3.5.3-orange.svg)](https://spark.apache.org/)
[![Delta Lake 3.3.2](https://img.shields.io/badge/delta-3.3.2-green.svg)](https://delta.io/)
[![Tests](https://img.shields.io/badge/tests-256%20passed-brightgreen.svg)](tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/omarbecerrasierra/databricks-local.svg?style=social&label=Star)](https://github.com/omarbecerrasierra/databricks-local)

> **⚠️ DISCLAIMER**: This is an **unofficial, independent, open-source project** for local development and testing. It is **NOT affiliated with, endorsed by, or supported by Databricks, Inc.** This project does **NOT** contain any proprietary Databricks code or images. Databricks® and Unity Catalog™ are trademarks of Databricks, Inc., used here only under nominative fair use for identification and interoperability. See the [Legal Disclaimer](#%EF%B8%8F-legal-disclaimer) and [NOTICE](NOTICE) file.

A local development environment that provides Unity Catalog-style and DBUtils-compatible APIs, allowing you to develop and test Apache Spark notebooks and ETL pipelines locally using **exclusively open-source technologies** (PySpark 3.5.3 + Delta Lake 3.3.2) without requiring any cloud workspace.

## 🎯 Features

### Unity Catalog Emulation
- **3-level namespace**: `catalog.schema.table`
- **Catalogs**: CREATE, DROP, DESCRIBE, USE CATALOG
- **Schemas**: CREATE, DROP, DESCRIBE, USE SCHEMA, SHOW SCHEMAS
- **Tables**: Full Delta Lake support with metadata tracking
- **Volumes**: `/Volumes/` paths with MANAGED and EXTERNAL volumes
- **Functions**: User-defined function registry with DESCRIBE FUNCTION
- **Grants & Permissions**: GRANT, REVOKE, DENY, SHOW GRANTS with audit logging
- **Tags**: SET/UNSET TAGS, COMMENT ON support for tables and volumes
- **Groups**: CREATE GROUP, ALTER GROUP ADD/REMOVE USER
- **Table Recovery**: UNDROP TABLE, SHOW TABLES DROPPED
- **Lineage Tracking**: Track data lineage between tables
- **Information Schema**: `information_schema.catalogs`, `schemata`, `tables`, `volumes`, `table_privileges`, `routines`
- **Delta Sharing**: No-op commands for CREATE SHARE, CREATE RECIPIENT
- **External Infrastructure**: No-op support for EXTERNAL LOCATION, STORAGE CREDENTIAL, SERVICE CREDENTIAL, CONNECTION, CLEAN ROOM

### DBUtils Complete Shim
- **`dbutils.credentials`**: Mock credential operations
- **`dbutils.secrets`**: In-memory secret management with scopes
- **`dbutils.widgets`**: Text, dropdown, combobox, and multiselect widgets
- **`dbutils.fs`**: Full filesystem operations supporting `dbfs:/`, `/Volumes/`, and local paths
  - Operations: ls, mkdirs, put, head, cp, rm, mv
  - FileInfo objects with path, name, size, modificationTime
- **`dbutils.notebook`**: Run, exit with value support
- **`dbutils.jobs.taskValues`**: Task value exchange
- **`dbutils.data.summarize`**: DataFrame profiling

### Delta Lake Features
- Time travel with `versionAsOf` and `timestampAsOf`
- MERGE, UPDATE, DELETE operations
- History tracking
- OPTIMIZE and VACUUM (local mode)
- Change Data Feed (CDF) support

### Development Modes
- **Local Mode**: Pure Python/PySpark development without Docker
- **Docker Mode**: Full environment with MinIO S3 + PostgreSQL Hive Metastore
- **Notebook Support**: Jupyter integration with `display()`, `spark`, `dbutils`, `uc`

## 📋 Requirements

- Python 3.11 or higher
- Java 11 or higher (for PySpark)
- Docker & Docker Compose (optional, for full environment)

## 🚀 Quick Start

### Local Installation

```bash
# Clone the repository
git clone https://github.com/omarbecerrasierra/databricks-local.git
cd databricks-local

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest tests/ -v
```

Output:
```
======================= 256 passed in 57.26s ========================
```

### Use in Notebooks

```python
import sys
sys.path.insert(0, "/path/to/databricks-local")

from databricks_shim import inject_notebook_context

# Inject globals: spark, dbutils, display, sc, uc
inject_notebook_context("MyNotebook")

# Now you can use Unity Catalog and DBUtils APIs
print(f"Current catalog: {uc.get_current_catalog()}")

# Create Unity Catalog resources
uc.sql("CREATE CATALOG IF NOT EXISTS analytics")
uc.sql("CREATE SCHEMA IF NOT EXISTS analytics.bronze")
uc.sql("CREATE VOLUME IF NOT EXISTS analytics.bronze.raw_data")

# Use dbutils
dbutils.fs.mkdirs("/Volumes/analytics/bronze/raw_data/files")
dbutils.secrets.get("my_scope", "api_key")

# Delta Lake operations
df.write.format("delta").mode("overwrite").save("/tmp/delta_table")
spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta_table").show()
```

See [notebooks/analysis.ipynb](notebooks/analysis.ipynb) for a complete example.

## 🐳 Docker Mode (Full Environment)

For S3-compatible storage (MinIO) and PostgreSQL Hive Metastore:

```bash
# Start services
docker compose up -d --build

# Run ETL pipeline in container
docker compose exec spark python main.py

# Stop services
docker compose down
```

The Docker environment includes:
- **MinIO**: S3-compatible object storage at `http://localhost:9000`
- **PostgreSQL**: Hive Metastore backend
- **Spark**: Configured with Delta Lake, Hive support, and AWS SDK

## 📁 Project Structure

```
databricks-local/
├── databricks_shim/          # Core Unity Catalog + DBUtils implementation
│   ├── __init__.py           # Main exports
│   ├── connect.py            # SparkSession factory
│   ├── unity_catalog.py      # Unity Catalog implementation (~1900 lines)
│   └── utils.py              # DBUtils implementation (~600 lines)
├── notebooks/
│   └── analysis.ipynb        # Complete demo notebook (28 cells)
├── tests/
│   ├── conftest.py           # Pytest fixtures
│   ├── test_etl.py           # ETL pipeline tests
│   └── test_unity_catalog.py # UC tests (256 tests)
├── main.py                   # Medallion ETL demo (Docker mode)
├── docker-compose.yml        # Docker services
├── Dockerfile                # Spark + Delta environment
├── pyproject.toml            # Project metadata & dependencies
└── README.md                 # This file
```

## 🧪 Testing

The project includes comprehensive test coverage:

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_unity_catalog.py -v

# Run with coverage
pytest tests/ --cov=databricks_shim --cov-report=html
```

### Test Coverage

- **Unity Catalog**: 256 tests covering all SQL commands and Python APIs
  - Catalogs, schemas, tables, volumes
  - Grants, tags, functions, groups
  - UNDROP, lineage, information_schema
  - Delta Sharing and cloud infrastructure no-ops
- **ETL Pipeline**: 4 tests for Medallion architecture
- **Integration Tests**: Notebook execution validation

## 📚 Documentation

### Unity Catalog API

```python
# Catalog operations
uc.create_catalog("analytics", comment="Analytics catalog", if_not_exists=True)
uc.list_catalogs()
uc.set_current_catalog("analytics")
uc.get_current_catalog()

# Schema operations
uc.create_schema("analytics", "bronze", comment="Bronze layer", if_not_exists=True)
uc.list_schemas("analytics")
uc.set_current_schema("bronze")
uc.describe_schema("analytics", "bronze")

# Volume operations
uc.sql("CREATE VOLUME analytics.bronze.raw_data")
path = uc.volume_path("analytics", "bronze", "raw_data", "file.csv")

# Functions
uc.create_function("main", "default", "clean_text",
                   definition="TRIM(LOWER(input))",
                   description="Text cleaning function")
uc.list_functions("main", "default")

# Grants
uc.sql("GRANT SELECT ON TABLE main.events TO analysts")
uc.sql("DENY INSERT ON TABLE main.events TO viewer_role")
uc.audit_log().show()

# Tags
uc.sql("ALTER TABLE main.events SET TAGS ('pii'='true', 'team'='data-eng')")
uc.get_tags("main.events")

# Lineage
uc.track_lineage("bronze.raw", "silver.clean", "TABLE")
uc.lineage_as_dataframe().show()
```

### DBUtils API

```python
# Secrets
dbutils.secrets.get("scope", "key")
dbutils.secrets.listScopes()

# Widgets
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.get("env")
dbutils.widgets.getAll()

# Filesystem
dbutils.fs.ls("/Volumes/analytics/bronze/raw_data/")
dbutils.fs.put("/dbfs/tmp/file.txt", "content", overwrite=True)
dbutils.fs.cp("source", "dest")
dbutils.fs.rm("/dbfs/tmp/", recurse=True)

# Data
dbutils.data.summarize(df)  # DataFrame profiling
```

### SQL Commands

Unity Catalog SQL commands are supported (based on open-source Delta Lake and Apache Spark):

```sql
-- Catalogs
CREATE CATALOG IF NOT EXISTS analytics;
USE CATALOG analytics;
SHOW CATALOGS;
DESCRIBE CATALOG analytics;

-- Schemas
CREATE SCHEMA bronze COMMENT 'Bronze layer';
USE SCHEMA bronze;
SHOW SCHEMAS IN analytics;

-- Volumes
CREATE VOLUME raw_data;
SHOW VOLUMES IN analytics.bronze;
DESCRIBE VOLUME analytics.bronze.raw_data;

-- Grants
GRANT SELECT ON TABLE events TO analysts;
REVOKE SELECT ON TABLE events FROM analysts;
SHOW GRANTS ON TABLE events;

-- Functions
CREATE FUNCTION clean_text AS 'TRIM(LOWER(input))';
SHOW FUNCTIONS IN main.default;
DESCRIBE FUNCTION main.default.clean_text;

-- Tags
ALTER TABLE events SET TAGS ('pii'='true');
SHOW TAGS ON TABLE events;

-- Groups
CREATE GROUP data_engineers;
ALTER GROUP data_engineers ADD USER user@example.com;
SHOW GROUPS;
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file for Docker mode:

```env
# MinIO S3
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://minio:9000
AWS_REGION=us-east-1

# PostgreSQL Hive Metastore
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=metastore_db
POSTGRES_USER=hive
POSTGRES_PASSWORD=hive_password
```

For local mode, these are automatically disabled in notebooks.

## 🛠️ Development

### Adding New Features

1. **Unity Catalog**: Edit `databricks_shim/unity_catalog.py`
   - Add regex pattern: `_PAT_YOUR_COMMAND = re.compile(...)`
   - Add handler in `sql()` method
   - Add Python API method if needed

2. **DBUtils**: Edit `databricks_shim/utils.py`
   - Extend the appropriate Mock class

3. **Tests**: Add tests to `tests/test_unity_catalog.py`

### Code Style

```bash
# Format code
black databricks_shim/ tests/

# Lint
ruff check databricks_shim/ tests/
```

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for new functionality
4. Ensure all tests pass (`pytest tests/ -v`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [Apache Spark™ 3.5.3](https://spark.apache.org/) (Apache License 2.0)
- Uses [Delta Lake 3.3.2](https://delta.io/) (Apache License 2.0)
- API patterns inspired by publicly available documentation (for interoperability and learning purposes only)
- **No proprietary code** — built entirely on open-source software
- **Not affiliated with or endorsed by Databricks, Inc.**

## 📞 Community Support

> **Note**: This is a community-driven, open-source project with NO official support.

- **Issues**: [GitHub Issues](https://github.com/omarbecerrasierra/databricks-local/issues)
- **Discussions**: [GitHub Discussions](https://github.com/omarbecerrasierra/databricks-local/discussions)
- **No SLA or guarantees** - best-effort community support only

For official enterprise support and production-ready solutions, visit [Databricks.com](https://databricks.com/).

## 🗺️ Roadmap

- [ ] MLflow integration for experiment tracking
- [ ] Web UI for catalog browsing
- [ ] Additional storage backends (Azure Blob, GCS)
- [ ] SQL Analytics endpoint patterns
- [ ] Workflows/Jobs scheduling patterns
- [ ] Extended Delta Lake features (CDF, Z-ordering)

---

## ⚖️ Legal Disclaimer

### Trademarks and Nominative Fair Use

- **Databricks®** is a registered trademark of Databricks, Inc.
- **Unity Catalog™** is a trademark of Databricks, Inc.
- **Delta Lake™** is a trademark of The Linux Foundation.
- **Apache Spark™** is a trademark of The Apache Software Foundation.
- All other trademarks, service marks, and company names are the property of their respective owners.

Use of these names in this project is solely for **identification and interoperability purposes** under nominative fair use. This project does not imply any affiliation with, endorsement by, or sponsorship from the trademark holders.

### No Affiliation

- **This project is NOT affiliated with, endorsed by, sponsored by, or supported by Databricks, Inc.**
- This is an independent, open-source project created for educational and local development purposes.
- This project does **NOT** contain, redistribute, or derive from any proprietary Databricks code, binaries, images, or services.
- The authors have no relationship with Databricks, Inc.

### No Official Support

- This software is provided "AS IS" without warranty of any kind.
- The authors are not responsible for any issues arising from the use of this software.
- This project does not grant any licenses to Databricks products or services.
- For official Databricks products and enterprise support, visit [databricks.com](https://databricks.com/).

### Intended Use

- **Local development and testing only** — not for production use.
- Educational purposes and learning data engineering patterns with open-source tools.
- Prototyping PySpark + Delta Lake pipelines before deploying to any cloud environment.
- Unit testing ETL pipelines without cloud infrastructure.

### Open Source Components

This project is built **entirely** on open-source technologies. No proprietary software is included or required:
- Apache Spark™ 3.5.3 (Apache License 2.0)
- Delta Lake 3.3.2 (Apache License 2.0)
- Python and PySpark
- PostgreSQL (PostgreSQL License)
- MinIO (AGPL v3)

For the full list of third-party components and their licenses, see the [NOTICE](NOTICE) file.

**For production workloads and enterprise features, please use official [Databricks](https://databricks.com/) services.**
