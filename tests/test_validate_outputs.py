from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from validate_outputs import (
    expected_counts_from_raw, inspect_parquet, sql_literal,
    validate_duckdb_relations, validate_outputs,
)


@pytest.mark.parametrize("ids,reason", [([], "empty"), ([1, 1], "duplicate"), ([None], "NULL")])
def test_parquet_reader_rejects_empty_duplicate_or_null_keys(tmp_path, ids, reason):
    path = tmp_path / "fixture.parquet"
    pq.write_table(pa.table({"company_id": pa.array(ids, type=pa.int64())}), path)
    with duckdb.connect(":memory:") as connection, pytest.raises(ValueError, match=reason):
        inspect_parquet(connection, "fixture", path, ["company_id"])


def test_parquet_reader_rejects_corrupt_content(tmp_path):
    path = tmp_path / "fixture.parquet"
    path.write_bytes(b"This exists but is not Parquet")
    with duckdb.connect(":memory:") as connection, pytest.raises(ValueError, match="unreadable"):
        inspect_parquet(connection, "fixture", path, ["company_id"])


@pytest.fixture
def output_root(tmp_path):
    raw = tmp_path / "data" / "raw"
    gold = tmp_path / "data" / "gold"
    raw.mkdir(parents=True)
    gold.mkdir(parents=True)
    pd.DataFrame({"company_id": [1, 1], "sector": ["Technology", "Technology"]}).to_csv(raw / "companies.csv", index=False)
    pd.DataFrame({"company_id": [1, 1], "date": ["2024-01-02", "2024-01-03"]}).to_csv(raw / "daily_prices.csv", index=False)
    pd.DataFrame({"company_id": [1], "fiscal_quarter": ["2024-Q1"]}).to_csv(raw / "quarterly_fundamentals.csv", index=False)
    tables = {
        "companies_clean": "select 1 as company_id",
        "daily_price_features": "select 1 as company_id, d as date, 100.0 as close_price, 0 as volume from (values (date '2024-01-02'), (date '2024-01-03')) t(d)",
        "quarterly_fundamental_features": "select 1 as company_id, '2024-Q1' as fiscal_quarter",
        "mart_company_quarterly_metrics": "select '1-2024-Q1' as company_quarter_key",
        "mart_sector_financial_health": "select 'Technology-2024-Q1' as sector_quarter_key",
        "mart_ml_ready_features": "select '1-2024-Q1' as company_quarter_key, NULL::double as next_quarter_revenue_growth_qoq, NULL::integer as next_quarter_growth_flag, false as is_training_eligible, date '2024-05-15' as feature_available_date, NULL::date as next_quarter_outcome_available_date",
    }
    with duckdb.connect(str(gold / "finance_analytics.duckdb")) as connection:
        connection.execute("create schema finance")
        for name, query in tables.items():
            path = gold / f"{name}.parquet" if name.startswith("mart_") else tmp_path / "data" / "silver" / name / "part.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            connection.execute(f"copy ({query}) to {sql_literal(path)} (format parquet)")
            if name.startswith("mart_"):
                connection.execute(f"create view finance.{name} as select * from read_parquet({sql_literal(path)})")
    return tmp_path


def test_alternative_input_sizes_reconcile_without_fixed_counts(output_root):
    expected = expected_counts_from_raw(output_root)
    assert expected["companies_clean"] == 1  # exact company duplicate is harmless
    assert expected["daily_price_features"] == 2
    assert expected["mart_ml_ready_features"] == 1
    assert validate_outputs(output_root) == expected


def test_missing_output_row_is_detected_against_raw(output_root):
    path = output_root / "data/silver/daily_price_features/part.parquet"
    table = pq.read_table(path).slice(0, 1)
    pq.write_table(table, path)
    with pytest.raises(ValueError, match="expected 2 rows.*found 1"):
        validate_outputs(output_root)


def test_broken_external_duckdb_view_is_queried_and_rejected(output_root):
    path = output_root / "data/gold/finance_analytics.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("create or replace view finance.mart_company_quarterly_metrics as select error('broken external relation') as company_quarter_key")
    with pytest.raises(ValueError, match="Cannot query DuckDB mart"):
        validate_duckdb_relations(output_root)


def test_duckdb_content_must_match_gold_parquet(output_root):
    with duckdb.connect(str(output_root / "data/gold/finance_analytics.duckdb")) as connection:
        connection.execute("create or replace view finance.mart_company_quarterly_metrics as select 'wrong-key' as company_quarter_key")
    with pytest.raises(ValueError, match="differs from its gold Parquet"):
        validate_duckdb_relations(output_root)
