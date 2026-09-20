.PHONY: setup python-install validate migration-update deploy-dev

setup:
	python -m venv .venv
	.venv\Scripts\python -m pip install --upgrade pip
	.venv\Scripts\pip install -r requirements.txt

python-install:
	.venv\Scripts\pip install -r requirements.txt

validate:
	databricks bundle validate -t dev

migration-update:
	liquibase update --changelog-file migrations/master.yaml

deploy-dev:
	databricks bundle deploy -t dev