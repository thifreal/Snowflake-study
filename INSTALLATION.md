# Installation Guide

## Overview

The project has multiple `requirements*.txt` files for different use cases:

| File | Purpose | Use When |
|------|---------|----------|
| `requirements.txt` | Core + Development + Testing | Local development with all features |
| `requirements-prod.txt` | Core dependencies only | Production deployment, minimal footprint |
| `requirements-dev.txt` | Core + Dev tools | Development with testing and notebooks |

## Installation

### Option 1: Production (Recommended for CI/CD)

Minimal dependencies for running pipelines:

```bash
pip install -r requirements-prod.txt
```

**Includes:**
- Snowflake connectors
- Data processing (pandas, numpy)
- Logging and configuration

**Size:** ~500MB

### Option 2: Development (Recommended for local work)

Full development tools including testing and notebooks:

```bash
pip install -r requirements-dev.txt
```

**Includes:**
- Everything in requirements-prod.txt
- Testing tools (pytest, coverage)
- Development tools (black, pylint, flake8)
- Jupyter notebooks
- Visualization (matplotlib, seaborn)

**Size:** ~1.5GB

### Option 3: Complete (All features)

All available dependencies:

```bash
pip install -r requirements.txt
```

**Includes:**
- Everything in requirements-dev.txt
- Data validation (pandera, great-expectations)
- Monitoring (sentry-sdk)
- Orchestration tools (optional, commented out)

**Size:** ~2GB

## Troubleshooting Installation

### Python Version Issues

This project requires **Python 3.9+**. Check your version:

```bash
python --version
```

### Package Version Conflicts

If you get version conflicts, try:

```bash
pip install --upgrade --force-reinstall -r requirements-prod.txt
```

### Specific Package Issues

#### snowflake-connector-python
```bash
# Make sure you have required system libraries
pip install --upgrade snowflake-connector-python
```

#### Jupyter notebooks
```bash
# If jupyterlab doesn't work, try
pip install jupyterlab --upgrade
jupyter notebook
```

#### Dependencies not found
- Remove version restrictions: Change `==` to `>=` in requirements
- Use the base `requirements.txt` which is more flexible

### Virtual Environment Setup

**Using venv:**
```bash
python -m venv venv
source venv/bin/activate       # On Windows: venv\Scripts\activate
pip install -r requirements-dev.txt
```

**Using conda:**
```bash
conda create -n snowflake-study python=3.11
conda activate snowflake-study
pip install -r requirements-dev.txt
```

## Installing Optional Features

### Add Airflow Support

For Apache Airflow orchestration:

```bash
pip install apache-airflow-providers-snowflake
```

### Add Data Validation

For advanced data validation:

```bash
pip install pandera great-expectations
```

### Add Visualization

For additional plotting capabilities:

```bash
pip install plotly bokeh altair
```

## Verify Installation

Test that everything is installed correctly:

```bash
# Test Snowflake connection
python -c "from snowflake.connector import connect; print('✓ Snowflake connector OK')"

# Test pandas
python -c "import pandas; print(f'✓ Pandas {pandas.__version__} OK')"

# Test development tools (if using requirements-dev.txt)
python -c "import pytest; print(f'✓ Pytest {pytest.__version__} OK')"
python -c "import black; print(f'✓ Black {black.__version__} OK')"

# If using Jupyter
jupyter --version
```

## Dependency Tree

Visualize the dependency tree:

```bash
pip install pipdeptree
pipdeptree --graph-output png > dependencies.png
```

## Contributing

When adding new dependencies:

1. Add to `requirements.txt` for core features
2. Add to `requirements-dev.txt` for dev-only tools
3. Update this guide with any special instructions
4. Test installation in clean environment

## Platform-Specific Notes

### Windows

Some packages may require Visual C++ build tools:
- [Visual C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

### MacOS (Apple Silicon)

For M1/M2 Macs, use conda for better compatibility:
```bash
conda install -c conda-forge snowflake-connector-python
pip install -r requirements-dev.txt
```

### Linux

Most packages work out-of-the-box. If you get build errors:

```bash
# Ubuntu/Debian
sudo apt-get install python3-dev build-essential

# Fedora/RHEL
sudo dnf install python3-devel gcc gcc-c++
```

## Getting Help

### Check installed packages:
```bash
pip list
```

### Show package details:
```bash
pip show snowflake-connector-python
```

### Downgrade/Upgrade specific package:
```bash
pip install snowflake-connector-python==3.4.1
pip install --upgrade snowflake-connector-python
```

### Generate requirements from current environment:
```bash
pip freeze > requirements-frozen.txt
```

---

**Last Updated**: April 15, 2026
