# Architecture

## Purpose

This project demonstrates a local financial lakehouse workflow built from scratch with synthetic data. The goal is to show how raw data can be generated, cleaned, modeled, and validated through a layered analytics engineering pipeline.

## End-To-End Flow

```text
Synthetic data generation
    ->
Raw CSV landing zone
    ->
PySpark ETL and feature engineering
    ->
Silver Parquet datasets
    ->
dbt transformations on DuckDB
    ->
Gold analytics marts and tests
```

## Layers

### Raw Layer

The raw layer contains three CSV files:

- `companies.csv`
- `daily_prices.csv`
- `quarterly_fundamentals.csv`

These files are generated locally by `src/generate_synthetic_data.py`. The generator introduces a small number of duplicates, missing values, and inconsistent sector casing to create realistic ETL cleaning work.

### Silver Layer

The silver layer is created by `src/spark_etl.py` and stored as Parquet in `data/silver/`.

Datasets:

- `companies_clean`
- `daily_price_features`
- `quarterly_fundamental_features`

Silver responsibilities:

- schema validation
- type casting
- duplicate removal
- value standardization
- missing value handling
- time-series feature engineering with Spark window functions
- native Spark Parquet writes into dataset folders with part files

### Gold Layer

The gold layer is built by dbt with DuckDB and stored as Parquet in `data/gold/`.

Marts:

- `mart_company_quarterly_metrics`
- `mart_sector_financial_health`
- `mart_ml_ready_features`

Gold responsibilities:

- expose business-friendly analytics tables
- calculate financial ratios
- aggregate sector performance
- create an ML-ready feature dataset without training a model
- enforce data quality through dbt tests

## Key Design Choices

### Local-Only Execution

The project runs entirely on a local machine. It avoids cloud services, external APIs, managed platforms, dashboards, and machine learning delivery so the implementation stays focused and easy to explain.

### Spark For ETL, dbt For Modeling

Spark handles heavy data preparation and window-based time-series calculations. dbt handles semantic modeling, testing, and documentation. This separation mirrors a common analytics engineering pattern.

On Windows, the repository includes a small Spark support class so the silver Parquet layer can still be written with Spark's native writer in a fully local workflow.

### Parquet As The Exchange Layer

Parquet sits between the Spark and dbt layers. It provides a clean boundary between ETL output and downstream modeling while staying efficient for analytical reads.

### DuckDB As The Local Warehouse

DuckDB is used as the execution engine for dbt. It can read Parquet directly and persist local relations without infrastructure overhead.

## Main Transformations

### PySpark Transformations

- standardize `ticker` values to uppercase
- standardize `sector` values to a controlled list
- fill missing price and fundamentals values with transparent rules
- compute `daily_return`
- compute `moving_average_20d`
- compute `rolling_volatility_20d`
- compute `previous_quarter_revenue`
- compute `revenue_growth_qoq`

### dbt Transformations

- create staging interfaces over silver Parquet data
- aggregate daily price data into quarterly market features
- calculate financial ratios
- join company, market, and fundamentals data at the company-quarter grain
- create sector-level summaries
- create an ML-ready feature mart

## Validation

Validation happens in two places:

- dbt schema tests confirm key quality rules for staged, intermediate, and mart models.
- `src/validate_outputs.py` confirms that expected silver datasets, gold Parquet marts, and DuckDB relations exist after a full run.
