# Contributing to databricks-local

Thank you for your interest in contributing! This guide will help you get started.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/databricks-local.git
   cd databricks-local
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Linux/macOS
   # .venv\Scripts\activate    # Windows
   ```

3. **Install in development mode**
   ```bash
   pip install -e ".[dev]"
   ```

4. **Run the test suite**
   ```bash
   pytest tests/ -v
   ```

## How to Contribute

### Reporting Bugs

- Open an [issue](../../issues) with a clear description
- Include: Python version, PySpark version, OS, and steps to reproduce
- Attach error logs if applicable

### Suggesting Features

- Check existing [issues](../../issues) first to avoid duplicates
- Describe the use case and how it relates to Unity Catalog or DBUtils APIs
- If possible, reference the official Databricks documentation for the feature

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Add or update tests in `tests/`
5. Run the test suite: `pytest tests/ -v`
6. Commit with a clear message: `git commit -m "Add: short description"`
7. Push and open a Pull Request

### Code Style

- Follow PEP 8 conventions
- Use type hints where practical
- Add docstrings to public methods
- Keep functions focused and small

### Testing

- All new features must include tests
- Tests go in the `tests/` directory
- Use `pytest` fixtures from `conftest.py` for Spark sessions and UC instances
- Aim for tests that run without Docker (pure Python + PySpark)

## Project Structure

```
databricks_shim/
├── __init__.py          # Package exports, inject_notebook_context()
├── connect.py           # SparkSession builder
├── unity_catalog.py     # Unity Catalog shim (~2000+ lines)
└── utils.py             # DBUtils shim (secrets, widgets, fs, etc.)
tests/
├── conftest.py          # Shared fixtures
├── test_etl.py          # ETL integration tests
└── test_unity_catalog.py # 256+ UC & DBUtils tests
notebooks/
└── analysis.ipynb       # Comprehensive demo notebook
```

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
