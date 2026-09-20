VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: setup test lint validate migration-update deploy-dev clean

# Create Python virtual environment and install dependencies
setup:
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PIP) install -r requirements.txt

# Run unit tests
test:
	$(PYTHON) -m pytest tests/unit

# Run Ruff linting
lint:
	$(PYTHON) -m ruff check .

# Validate Databricks Bundle for DEV
validate:
	databricks bundle validate -t dev

# Run Liquibase migrations
migration-update:
	liquibase update --changelog-file migrations/master.yaml

# Deploy Databricks Bundle to DEV
deploy-dev:
	databricks bundle deploy -t dev

# Remove Python virtual environment
clean:
	rm -rf $(VENV)
