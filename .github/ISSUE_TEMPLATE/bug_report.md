---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: bug
assignees: ''

---

> **Note**: This is an unofficial open-source project, NOT affiliated with Databricks, Inc.

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Run command '...'
2. Execute code '...'
3. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Actual behavior**
What actually happened.

**Code Sample**
```python
# Minimal code to reproduce the issue
from databricks_shim import inject_notebook_context
inject_notebook_context("test")

# Your code here that triggers the bug
```

**Error Message**
```
Paste the full error traceback here
```

**Environment:**
 - OS: [e.g. Ubuntu 22.04, macOS 14, Windows 11]
 - Python Version: [e.g. 3.11.5]
 - Package Version: [e.g. 0.4.0]
 - PySpark Version: [from `pip show pyspark`]
 - Delta Lake Version: [from `pip show delta-spark`]
 - Mode: [Local / Docker]

**Additional context**
Add any other context about the problem here.
