from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[1]
EXPECTED_DBT_RELATIONS = {
    "mart_company_quarterly_metrics",
    "mart_sector_financial_health",
    "mart_ml_ready_features",
}


SILVER_KEYS = {
    "companies_clean": ["company_id"],
    "daily_price_features": ["company_id", "date"],
    "quarterly_fundamental_features": ["company_id", "fiscal_quarter"],
}
GOLD_KEYS = {
    "mart_company_quarterly_metrics": ["company_quarter_key"],
    "mart_sector_financial_health": ["sector_quarter_key"],
    "mart_ml_ready_features": ["company_quarter_key"],
}


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("\\", "/").replace("'", "''") + "'"


def inspect_parquet(connection, name: str, path: Path, keys: list[str]) -> int:
    """Scan all columns/pages, rather than trusting filenames or footer row counts."""
    try:
        connection.execute(f"create temporary view {name} as select * from read_parquet({sql_literal(path)})")
        columns = {row[0] for row in connection.execute(f"describe {name}").fetchall()}
        if not set(keys) <= columns:
            raise ValueError(f"{name}: missing key columns {sorted(set(keys) - columns)}")
        reader = connection.execute(f"select * from {name}").to_arrow_reader(65536)
        count = sum(batch.num_rows for batch in reader)
        if count == 0:
            raise ValueError(f"{name}: empty Parquet output")
        key_list = ", ".join(keys)
        distinct_count = connection.execute(f"select count(*) from (select distinct {key_list} from {name})").fetchone()[0]
        nulls = connection.execute(f"select count(*) from {name} where " + " or ".join(f"{k} is null" for k in keys)).fetchone()[0]
        if count != distinct_count or nulls:
            raise ValueError(f"{name}: duplicate or NULL business keys")
        return count
    except duckdb.Error as error:
        raise ValueError(f"{name}: unreadable or invalid Parquet at {path}: {error}") from error


def assert_no_rows(connection, query: str, message: str) -> None:
    if connection.execute(query).fetchone():
        raise ValueError(message)


def validate_silver_outputs(root_dir: Path = ROOT_DIR) -> dict[str, int]:
    counts = {}
    with duckdb.connect(":memory:") as connection:
        for name, keys in SILVER_KEYS.items():
            path = root_dir / "data" / "silver" / name
            if not list(path.glob("*.parquet")):
                raise FileNotFoundError(f"Missing silver Parquet output: {name}")
            counts[name] = inspect_parquet(connection, name, path / "*.parquet", keys)
        assert_no_rows(connection, """
            select 1 from daily_price_features
            where close_price is null or not isfinite(close_price) or close_price <= 0
               or (volume is not null and (not isfinite(volume) or volume < 0)) limit 1
            """, "Silver contains invalid price/volume values")
        for name in ("daily_price_features", "quarterly_fundamental_features"):
            assert_no_rows(connection, f"""
                select 1 from {name} f left join companies_clean c using (company_id)
                where c.company_id is null limit 1
                """, f"{name}: unknown company_id")
    return counts


def validate_gold_outputs(root_dir: Path = ROOT_DIR) -> dict[str, int]:
    counts = {}
    with duckdb.connect(":memory:") as connection:
        for name, keys in GOLD_KEYS.items():
            path = root_dir / "data" / "gold" / f"{name}.parquet"
            if not path.is_file():
                raise FileNotFoundError(f"Missing gold mart output: {path.name}")
            counts[name] = inspect_parquet(connection, name, path, keys)
        assert_no_rows(connection, """
            select 1 from mart_ml_ready_features
            where next_quarter_growth_flag is distinct from
                case when next_quarter_revenue_growth_qoq is null then null
                     when next_quarter_revenue_growth_qoq > 0 then 1 else 0 end
               or is_training_eligible is null
               or (is_training_eligible and
                   (next_quarter_growth_flag is null
                    or next_quarter_outcome_available_date is null
                    or feature_available_date is null
                    or next_quarter_outcome_available_date <= feature_available_date)) limit 1
            """, "Gold contains inconsistent labels/training eligibility")
    return counts


def validate_duckdb_relations(root_dir: Path = ROOT_DIR) -> dict[str, int]:
    path = root_dir / "data" / "gold" / "finance_analytics.duckdb"
    if not path.is_file():
        raise FileNotFoundError(f"DuckDB database not found at {path}")
    counts = {}
    with duckdb.connect(str(path), read_only=True) as connection:
        # dbt external views use repository-relative Parquet locations.
        connection.execute(f"set file_search_path = {sql_literal(root_dir)}")
        for name in sorted(EXPECTED_DBT_RELATIONS):
            try:
                counts[name] = connection.execute(f"select count(*) from finance.{name}").fetchone()[0]
                if counts[name] == 0:
                    raise ValueError(f"DuckDB mart {name} is empty")
                parquet = sql_literal(root_dir / "data" / "gold" / f"{name}.parquet")
                assert_no_rows(connection, f"""
                    (select * from finance.{name} except all select * from read_parquet({parquet}))
                    union all
                    (select * from read_parquet({parquet}) except all select * from finance.{name})
                    limit 1
                    """, f"DuckDB mart {name} differs from its gold Parquet output")
            except duckdb.Error as error:
                raise ValueError(f"Cannot query DuckDB mart finance.{name}: {error}") from error
    return counts


def expected_counts_from_raw(root_dir: Path = ROOT_DIR) -> dict[str, int]:
    """Derive cardinalities from supplied inputs, including valid alternative sizes."""
    with duckdb.connect(":memory:") as connection:
        for name in ("companies", "daily_prices", "quarterly_fundamentals"):
            path = root_dir / "data" / "raw" / f"{name}.csv"
            if not path.is_file():
                raise FileNotFoundError(f"Cannot reconcile without raw input: {path}")
            connection.execute(f"create view raw_{name} as select * from read_csv({sql_literal(path)}, header=true, all_varchar=true)")
        companies = connection.execute("select count(distinct cast(company_id as integer)) from raw_companies").fetchone()[0]
        prices = connection.execute("select count(*) from (select distinct cast(company_id as integer), cast(date as date) from raw_daily_prices)").fetchone()[0]
        quarters = connection.execute("select count(*) from (select distinct cast(company_id as integer), fiscal_quarter from raw_quarterly_fundamentals)").fetchone()[0]
        sectors = connection.execute("""
            select count(*) from (
                select distinct lower(trim(c.sector)), f.fiscal_quarter
                from raw_quarterly_fundamentals f join raw_companies c
                  on cast(f.company_id as integer) = cast(c.company_id as integer))
            """).fetchone()[0]
    return dict(companies_clean=companies, daily_price_features=prices,
                quarterly_fundamental_features=quarters, mart_company_quarterly_metrics=quarters,
                mart_ml_ready_features=quarters, mart_sector_financial_health=sectors)


def validate_outputs(root_dir: Path = ROOT_DIR) -> dict[str, int]:
    root_dir = root_dir.resolve()
    counts = validate_silver_outputs(root_dir) | validate_gold_outputs(root_dir)
    database_counts = validate_duckdb_relations(root_dir)
    expected = expected_counts_from_raw(root_dir)
    for name, count in counts.items():
        if count != expected[name]:
            raise ValueError(f"{name}: expected {expected[name]} rows from raw keys, found {count}")
        if name in database_counts and database_counts[name] != count:
            raise ValueError(f"{name}: DuckDB/Parquet row count mismatch")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Read and reconcile raw, silver, gold and DuckDB outputs.")
    parser.add_argument("--root", type=Path, default=ROOT_DIR, help="Pipeline workspace (defaults to this repository).")
    args = parser.parse_args()
    counts = validate_outputs(args.root)
    print(f"All silver and gold outputs validated successfully: {counts}")


if __name__ == "__main__":
    main()
