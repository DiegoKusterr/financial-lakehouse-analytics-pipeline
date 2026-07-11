PYTHON ?= python
DBT_PROJECT_DIR ?= dbt_finance
DBT_PROFILES_DIR ?= dbt_finance

.PHONY: install generate-data spark-etl dbt-run dbt-test dbt-docs build-demo preview-demo validate clean

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

generate-data:
	$(PYTHON) src/generate_synthetic_data.py

spark-etl:
	$(PYTHON) src/spark_etl.py

dbt-run:
	dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs:
	dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

build-demo:
	$(PYTHON) src/build_technical_demo.py

preview-demo:
	$(PYTHON) -m http.server 8080 --directory docs-demo

validate:
	$(PYTHON) src/validate_outputs.py

clean:
	$(PYTHON) -c "from pathlib import Path; import shutil; root = Path('.'); [path.unlink() for path in root.glob('data/raw/*.csv') if path.is_file()]; [shutil.rmtree(path) for path in (root / 'data' / 'silver').iterdir() if path.is_dir()]; [(shutil.rmtree(path) if path.is_dir() else path.unlink()) for path in (root / 'data' / 'gold').iterdir() if path.name != '.gitkeep']; [path.unlink() for pattern in ('*.duckdb', '*.duckdb.wal', '*.duckdb.tmp') for path in root.rglob(pattern) if path.is_file()]; [shutil.rmtree(path) for path in (root / 'dbt_finance' / 'target', root / 'dbt_finance' / 'logs', root / 'dbt_finance' / 'dbt_packages') if path.exists()]; [path.unlink() for path in (root / 'dbt_finance' / 'profiles.yml', root / 'dbt_finance' / '.user.yml') if path.exists()]; [shutil.rmtree(path) for path in root.rglob('__pycache__') if path.is_dir()]; [path.unlink() for path in root.rglob('*.pyc') if path.is_file()]; [shutil.rmtree(path) for path in (root / '.pytest_cache', root / '.ruff_cache') if path.exists()]"
