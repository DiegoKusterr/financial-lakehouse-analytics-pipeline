# Financial Lakehouse Analytics Pipeline

## Overview

This repository is a local financial lakehouse pipeline built around synthetic market and fundamentals data. It uses PySpark for raw-to-silver ETL, Parquet for analytical storage, and dbt with DuckDB to build tested marts with lineage-aware documentation.

The project is intentionally local-first. It focuses on reproducible analytics engineering, not cloud orchestration, BI dashboards, or model serving.

## Technical Demo

[dbt Docs & Technical Demo](https://diegokusterr.github.io/financial-lakehouse-analytics-pipeline/dbt-docs/#!/overview)

This link opens the static dbt Docs technical demo directly at the overview page. It is a static documentation build, not a live dashboard or live data application.

## Why This Project Exists

Many portfolio projects stop at notebooks or screenshots. This repository was built to show a fuller analytics workflow:

- synthetic raw data generation
- typed ingestion and ETL in PySpark
- Parquet-based intermediate storage
- dbt modeling with staging, intermediate, and mart layers
- local analytical execution in DuckDB
- documented lineage and repeatable data-quality checks

## Architecture

```text
Synthetic CSV generator
        |
        v
data/raw/
  - companies.csv
  - daily_prices.csv
  - quarterly_fundamentals.csv
        |
        v
PySpark ETL
  - schema checks
  - type casting
  - deduplication
  - standardization
  - window-based features
        |
        v
data/silver/ Parquet datasets
        |
        v
dbt + DuckDB
  - staging models
  - intermediate models
  - marts
  - schema tests
  - docs + lineage
        |
        v
data/gold/ analytical outputs
```

![dbt lineage graph](docs/assets/dbt_lineage_graph.png)

The public demo is generated from this dbt layer and exposes model lineage, documentation, sources, and tests without adding a separate dashboard tool.

## Key Engineering Decisions

- Synthetic source data keeps the repository runnable without external data contracts or licensed financial feeds.
- PySpark is used for the ETL layer so cleaning, typing, deduplication, and window features are expressed in distributed-style data transformations rather than notebook-only pandas code.
- Parquet separates storage from modeling and keeps the handoff between ETL and analytics layers explicit.
- DuckDB plus dbt keeps the analytical layer local, SQL-focused, and reproducible.
- A small helper class under `src/spark_support/` is included so local Spark Parquet writes stay stable on Windows.

## Data Model

The project uses three raw entities and three analytical marts.

Raw inputs:

- `companies`: company dimension data
- `daily_prices`: daily market observations
- `quarterly_fundamentals`: quarterly financial statement metrics

Final marts:

- `mart_company_quarterly_metrics`: company-quarter level analytical view
- `mart_sector_financial_health`: sector-level rollup
- `mart_ml_ready_features`: downstream-ready feature table for later modeling work

## PySpark Layer

The PySpark layer lives in `src/spark_etl.py` and creates the silver datasets.

Main responsibilities:

- validate required columns
- cast date and numeric fields
- remove duplicate rows
- standardize tickers and sector values
- apply transparent missing-value handling
- calculate daily returns
- calculate rolling price features
- prepare quarterly company features for downstream SQL modeling

## dbt and DuckDB Layer

The dbt project lives in `dbt_finance/` and uses DuckDB as the local analytical engine.

This layer demonstrates:

- staging models over the curated Parquet inputs
- intermediate transformations for reusable analytical logic
- mart models for reporting-ready and ML-ready outputs
- documentation and lineage through dbt Docs
- schema tests embedded in the model layer

## Data Quality and Testing

Quality checks are applied in more than one layer:

- schema and typing checks happen during the PySpark ETL step
- dbt schema tests validate model expectations in the SQL layer
- `src/validate_outputs.py` verifies that the expected final relations and artifacts exist
- the repository also includes a small automated test layer under `tests/`

## Tech Stack

- Python 3.11
- PySpark / Apache Spark
- Parquet
- dbt Core
- dbt-duckdb
- DuckDB
- Pandas and NumPy for synthetic data generation
- Make for repeatable local commands

## Selected Repository Structure

```text
financial-lakehouse-analytics-pipeline/
|-- data/
|   |-- raw/
|   |-- silver/
|   `-- gold/
|-- src/
|   |-- spark_support/
|   |-- generate_synthetic_data.py
|   |-- spark_etl.py
|   |-- build_technical_demo.py
|   `-- validate_outputs.py
|-- dbt_finance/
|   |-- dbt_project.yml
|   |-- profiles.yml.example
|   `-- models/
|-- docs/
|   |-- architecture.md
|   `-- data_dictionary.md
|-- docs-demo/
|-- tests/
|-- Makefile
`-- README.md
```

## Run Locally

1. Create and activate a virtual environment.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies.

```powershell
make install
```

3. Create a local dbt profile by copying `dbt_finance/profiles.yml.example` to `dbt_finance/profiles.yml` and adjusting local paths if needed.

4. Generate the synthetic raw data.

```powershell
make generate-data
```

5. Run the PySpark ETL.

```powershell
make spark-etl
```

6. Build the dbt models and tests.

```powershell
make dbt-run
make dbt-test
```

7. Generate docs, build the static demo, and validate outputs.

```powershell
make dbt-docs
python src/build_technical_demo.py
make validate
```

Optional local preview:

```powershell
python -m http.server 8080 --directory docs-demo
```

## Limitations

- The repository uses synthetic data, so it demonstrates pipeline design rather than real market coverage.
- The project is intentionally local-first and does not include orchestration, cloud infrastructure, or BI serving layers.
- The public demo is a static dbt Docs build, not a live query service.
