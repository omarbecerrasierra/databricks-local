# Contributing to PySpark Unity Catalog Local Development Environment

> **Important**: This project is NOT affiliated with Databricks, Inc. See [DISCLAIMER.md](DISCLAIMER.md) for full legal information.

First off, thank you for considering contributing to this project! 🎉

## Code of Conduct

This project aims to be welcoming to everyone. Please be respectful and constructive in all interactions.

## Legal Requirements

By contributing to this project, you:
- Confirm your contributions are original or properly licensed
- Agree NOT to include any Databricks proprietary code or intellectual property
- Acknowledge this project is NOT affiliated with Databricks, Inc.
- Accept the MIT License terms for your contributions
- Take responsibility for ensuring your code doesn't violate any copyrights or trademarks

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- **Clear title and description**
- **Steps to reproduce** the behavior
- **Expected vs actual behavior**
- **Environment details** (Python version, OS, etc.)
- **Code sample** if applicable

Example:
```markdown
**Bug**: GRANT command fails with multiple privileges

**Steps to reproduce**:
1. Run `uc.sql("GRANT SELECT, INSERT ON TABLE my_table TO user")`
2. See error

**Expected**: Command succeeds
**Actual**: ValueError raised

**Environment**: Python 3.11, macOS 14
```

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. Include:

- **Clear title** describing the enhancement
- **Detailed description** of the proposed functionality
- **Why this enhancement would be useful**
- **Example usage** if possible

### Pull Requests

1. **Fork** the repository
2. **Create a branch** from `main`:
   ```bash
   git checkout -b feature/my-new-feature
   ```

3. **Make your changes**:
   - Write clear, commented code
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

4. **Run tests** to ensure everything passes:
   ```bash
   pytest tests/ -v
   ```

5. **Commit** your changes:
   ```bash
   git commit -m "Add feature: description of feature"
   ```
   
   Use clear commit messages:
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `test:` for test additions/changes
   - `refactor:` for code refactoring

6. **Push** to your fork:
   ```bash
   git push origin feature/my-new-feature
   ```

7. **Open a Pull Request** with:
   - Clear title and description
   - Reference to any related issues
   - Summary of changes made

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/databricks-local.git
cd databricks-local

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest tests/ -v
```

## Testing Guidelines

- **Write tests** for all new features
- **Maintain coverage**: Aim for >90% test coverage
- **Test naming**: Use descriptive names like `test_create_catalog_if_not_exists`
- **Use fixtures**: Leverage pytest fixtures in `conftest.py`
- **Test both success and failure cases**

Example test:
```python
def test_create_catalog_with_comment(uc):
    """Test creating a catalog with a comment."""
    uc.sql("CREATE CATALOG analytics COMMENT 'Analytics catalog'")
    catalogs = uc.list_catalogs()
    assert any(c.name == "analytics" for c in catalogs)
    
    # Verify comment is stored
    result = uc.sql("DESCRIBE CATALOG analytics")
    rows = result.collect()
    assert any("Analytics catalog" in str(row) for row in rows)
```

## Code Style

- **Python**: Follow PEP 8
- **Imports**: Group stdlib, third-party, local imports
- **Docstrings**: Use for all public functions/classes
- **Type hints**: Add where it improves clarity
- **Comments**: Explain 'why', not 'what'

Example:
```python
def create_catalog(
    self,
    catalog_name: str,
    comment: str = "",
    if_not_exists: bool = False
) -> None:
    """Create a new catalog in Unity Catalog.
    
    Args:
        catalog_name: Name of the catalog to create
        comment: Optional description of the catalog
        if_not_exists: If True, don't raise error if catalog exists
        
    Raises:
        ValueError: If catalog exists and if_not_exists is False
    """
    # Implementation...
```

## Adding Unity Catalog Features

When adding new Unity Catalog SQL commands:

1. **Add regex pattern** in `unity_catalog.py`:
   ```python
   _PAT_MY_COMMAND = re.compile(
       r"MY\s+COMMAND\s+(\w+)",
       re.IGNORECASE
   )
   ```

2. **Add handler** in the `sql()` method:
   ```python
   # Check for MY COMMAND
   m = _PAT_MY_COMMAND.match(clean_query)
   if m:
       return self._handle_my_command(m)
   ```

3. **Implement handler**:
   ```python
   def _handle_my_command(self, match: re.Match) -> DataFrame:
       """Handle MY COMMAND SQL statement."""
       name = match.group(1)
       # Implementation...
   ```

4. **Add Python API** (if applicable):
   ```python
   def my_command(self, name: str) -> None:
       """Execute my command via Python API."""
       self.sql(f"MY COMMAND {name}")
   ```

5. **Write tests**:
   ```python
   class TestMyCommand:
       def test_my_command_sql(self, uc):
           uc.sql("MY COMMAND test")
           # Assertions...
       
       def test_my_command_python_api(self, uc):
           uc.my_command("test")
           # Assertions...
   ```

## Documentation

- Update **README.md** for new features
- Add **docstrings** to all public functions
- Include **usage examples** in docstrings
- Update **notebooks/** if demonstrating new features

## Project Structure

```
databricks_shim/
├── __init__.py          # Public API exports
├── connect.py           # SparkSession factory
├── unity_catalog.py     # UC implementation
│   ├── Patterns (_PAT_*)
│   ├── Handlers (_handle_*)
│   ├── Python API (create_*, list_*, etc.)
│   └── Information Schema
└── utils.py             # DBUtils implementation
    ├── FSMock           # dbutils.fs
    ├── SecretsMock      # dbutils.secrets
    ├── WidgetsMock      # dbutils.widgets
    └── DBUtilsShim      # Main class
```

## Questions?

Feel free to:
- Open a [Discussion](https://github.com/omarbecerrasierra/databricks-local/discussions)
- Ask in an [Issue](https://github.com/omarbecerrasierra/databricks-local/issues)
- Reach out to maintainers

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

**Important**: Contributors must NOT include any proprietary code, Databricks intellectual property, or copyrighted material. All contributions must be original work or properly licensed open-source code.

---

Thank you for contributing! 🚀
