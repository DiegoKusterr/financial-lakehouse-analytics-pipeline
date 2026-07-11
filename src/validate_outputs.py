from __future__ import annotations

from pathlib import Path

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = ROOT_DIR / "data" / "silver"
GOLD_DIR = ROOT_DIR / "data" / "gold"
DUCKDB_PATH = GOLD_DIR / "finance_analytics.duckdb"

EXPECTED_SILVER_DATASETS = (
    "companies_clean",
    "daily_price_features",
    "quarterly_fundamental_features",
)
EXPECTED_GOLD_FILES = (
    "mart_company_quarterly_metrics.parquet",
    "mart_sector_financial_health.parquet",
    "mart_ml_ready_features.parquet",
)
EXPECTED_DBT_RELATIONS = {
    "mart_company_quarterly_metrics",
    "mart_sector_financial_health",
    "mart_ml_ready_features",
}


def validate_silver_outputs() -> None:
    missing_datasets: list[str] = []

    for dataset in EXPECTED_SILVER_DATASETS:
        dataset_dir = SILVER_DIR / dataset
        parquet_files = list(dataset_dir.glob("*.parquet"))
        if not dataset_dir.exists() or not parquet_files:
            missing_datasets.append(dataset)

    if missing_datasets:
        missing = ", ".join(missing_datasets)
        raise FileNotFoundError(f"Missing silver Parquet outputs: {missing}")


def validate_gold_outputs() -> None:
    missing_files = [name for name in EXPECTED_GOLD_FILES if not (GOLD_DIR / name).exists()]
    if missing_files:
        raise FileNotFoundError(f"Missing gold mart outputs: {', '.join(missing_files)}")


def validate_duckdb_relations() -> None:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found at {DUCKDB_PATH}")

    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as connection:
        rows = connection.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema in ('finance', 'main')
            """
        ).fetchall()

    existing_relations = {row[0] for row in rows}
    missing_relations = sorted(EXPECTED_DBT_RELATIONS - existing_relations)

    if missing_relations:
        missing = ", ".join(missing_relations)
        raise FileNotFoundError(f"Missing dbt relations inside DuckDB: {missing}")


def main() -> None:
    validate_silver_outputs()
    validate_gold_outputs()
    validate_duckdb_relations()
    print("All silver and gold outputs validated successfully.")


if __name__ == "__main__":
    main()
