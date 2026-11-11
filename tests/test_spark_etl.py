from datetime import date, timedelta

import pytest

from conftest import string_frame
from spark_etl import (
    REQUIRED_COLUMNS, deduplicate_records, prepare_companies,
    prepare_daily_prices, prepare_quarterly_fundamentals,
)


def prices(spark, rows):
    return string_frame(spark, [dict(company_id=1, date=d, close_price=p, volume=v) for d, p, v in rows],
                        ["company_id", "date", "close_price", "volume"])


def fundamental(quarter="2024-Q1", **overrides):
    year, q = int(quarter[:4]), int(quarter[-1])
    row = dict(company_id=1, fiscal_quarter=quarter,
               available_date=f"{year if q < 4 else year + 1}-{(q * 3 + 2) if q < 4 else 2:02d}-14",
               revenue=100, gross_profit=60, operating_income=20, net_income=10,
               total_assets=200, total_liabilities=80, equity=120, operating_cash_flow=12)
    row.update(overrides)
    return row


def fundamentals(spark, rows):
    return string_frame(spark, rows, sorted(REQUIRED_COLUMNS["quarterly_fundamentals"]))


def test_daily_imputation_uses_only_observed_history(spark, companies):
    rows = [("2024-01-02", 100, 10), ("2024-01-03", None, None), ("2024-01-04", 200, 30)]
    actual = prepare_daily_prices(prices(spark, rows), companies).orderBy("date").collect()
    assert actual[1].close_price == 100
    assert actual[1].volume == 10
    assert actual[1].daily_return == 0
    assert actual[2].daily_return == 1
    assert actual[2].moving_average_20d == pytest.approx(133.3333)
    assert actual[2].rolling_volatility_20d == pytest.approx(2 ** -0.5)
    assert actual[0].daily_return is None
    assert actual[0].rolling_volatility_20d is None
    assert set(actual[0].asDict()) == {
        "company_id", "ticker", "company_name", "sector", "industry", "country", "date",
        "close_price", "volume", "daily_return", "moving_average_20d", "rolling_volatility_20d",
    }


def test_leading_missing_price_is_rejected_instead_of_backfilled(spark, companies):
    with pytest.raises(ValueError, match="without an observed past price"):
        prepare_daily_prices(prices(spark, [("2024-01-02", None, 10), ("2024-01-03", 200, 20)]), companies)


@pytest.mark.parametrize("column,value", [
    ("close_price", -1), ("close_price", 0), ("close_price", "NaN"),
    ("close_price", "Infinity"), ("close_price", "-Infinity"), ("close_price", "bad"),
    ("volume", -1), ("volume", "NaN"), ("volume", "Infinity"), ("volume", "-Infinity"),
    ("volume", 2**63),
])
def test_invalid_market_values_fail_before_writing(spark, companies, column, value):
    row = dict(company_id=1, date="2024-01-02", close_price=100, volume=10)
    row[column] = value
    with pytest.raises(ValueError, match="invalid numeric"):
        prepare_daily_prices(string_frame(spark, [row], list(row)), companies)


def test_zero_volume_is_a_valid_observation(spark, companies):
    result = prepare_daily_prices(prices(spark, [("2024-01-02", 100, 0)]), companies).first()
    assert result.volume == 0


def test_small_positive_price_is_not_rounded_to_zero(spark, companies):
    result = prepare_daily_prices(prices(spark, [("2024-01-02", 0.00001, 10)]), companies).first()
    assert result.close_price == 0.00001


def test_leading_missing_volume_stays_null(spark, companies):
    result = prepare_daily_prices(prices(spark, [("2024-01-02", 100, None), ("2024-01-03", 101, 100)]), companies)
    assert result.orderBy("date").first().volume is None


def test_rolling_average_uses_only_twenty_observations(spark, companies):
    rows = [((date(2024, 1, 2) + timedelta(days=i)).isoformat(), i + 1, 10) for i in range(21)]
    last = prepare_daily_prices(prices(spark, rows), companies).orderBy("date").collect()[-1]
    assert last.moving_average_20d == 11.5  # prices 2..21; the first price has left the window
    assert last.daily_return == pytest.approx(0.05)


def test_fundamentals_preserve_nulls_zeros_and_publication_dates(spark, companies):
    raw = fundamentals(spark, [fundamental(gross_profit=None, operating_cash_flow=None, net_income=0),
                               fundamental("2024-Q2", gross_profit=80, operating_cash_flow=None, revenue=120)])
    rows = prepare_quarterly_fundamentals(raw, companies).orderBy("fiscal_quarter").collect()
    assert rows[0].gross_profit is None  # Q2's observed profit must not fill Q1.
    assert all(r.operating_cash_flow is None for r in rows)
    assert rows[0].net_income == 0
    assert rows[0].available_date == date(2024, 5, 14)
    assert rows[1].feature_available_date == date(2024, 8, 14)
    assert rows[1].previous_quarter_revenue == 100
    assert rows[1].revenue_growth_qoq == pytest.approx(0.2)
    assert rows[0].previous_quarter_revenue is None
    assert rows[0].revenue_growth_qoq is None
    assert {"available_date", "feature_available_date", "fiscal_quarter_start_date"} <= set(rows[0].asDict())


def test_missing_intermediate_quarter_does_not_produce_qoq_growth(spark, companies):
    raw = fundamentals(spark, [fundamental(), fundamental("2024-Q3", revenue=150)])
    last = prepare_quarterly_fundamentals(raw, companies).orderBy("fiscal_quarter").collect()[-1]
    assert last.previous_quarter_revenue is None
    assert last.revenue_growth_qoq is None


def test_late_prior_report_delays_feature_availability(spark, companies):
    raw = fundamentals(spark, [fundamental(available_date="2024-09-01"), fundamental("2024-Q2")])
    last = prepare_quarterly_fundamentals(raw, companies).orderBy("fiscal_quarter").collect()[-1]
    assert last.feature_available_date == date(2024, 9, 1)


@pytest.mark.parametrize("quarter", ["2024-Q10", "2024-Q0", "2024-Q5", "x2024-Q1", "2024-Q1x", "24-Q1", "2024-q1", "2024-Q1 ", "2024-Q1\n", "0000-Q1", None])
def test_invalid_quarters_are_rejected(spark, companies, quarter):
    row = fundamental()
    row["fiscal_quarter"] = quarter
    with pytest.raises(ValueError, match="invalid fiscal_quarter"):
        prepare_quarterly_fundamentals(fundamentals(spark, [row]), companies)


def test_quarter_publication_cannot_precede_period_end(spark, companies):
    with pytest.raises(ValueError, match="precedes quarter end"):
        prepare_quarterly_fundamentals(fundamentals(spark, [fundamental(available_date="2024-01-15")]), companies)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "bad"])
def test_invalid_fundamental_values_are_rejected(spark, companies, value):
    with pytest.raises(ValueError, match="invalid numeric"):
        prepare_quarterly_fundamentals(fundamentals(spark, [fundamental(gross_profit=value)]), companies)


def test_exact_repeats_are_removed_and_conflicts_rejected(spark):
    raw = spark.createDataFrame([(1, "2024-Q1", 100), (1, "2024-Q1", 100), (2, "2024-Q1", 200)],
                                "company_id int, quarter string, revenue int")
    assert deduplicate_records(raw, ["company_id", "quarter"], "fixture").count() == 2
    conflict = raw.union(spark.createDataFrame([(1, "2024-Q1", 101)], raw.schema))
    with pytest.raises(ValueError, match="conflicting records.*company_id.*1"):
        deduplicate_records(conflict, ["company_id", "quarter"], "fixture")


def test_price_conflict_is_not_resolved_by_imputation(spark, companies):
    with pytest.raises(ValueError, match="daily_prices: conflicting records"):
        prepare_daily_prices(prices(spark, [("2024-01-02", 100, 10), ("2024-01-02", None, 10)]), companies)


def test_fundamental_conflict_is_rejected(spark, companies):
    with pytest.raises(ValueError, match="quarterly_fundamentals: conflicting records"):
        prepare_quarterly_fundamentals(fundamentals(spark, [fundamental(), fundamental(revenue=101)]), companies)


def test_null_sector_is_rejected(spark):
    raw = string_frame(spark, [dict(company_id=1, ticker="TEST", company_name="Test", sector=None,
                                    industry="Software", country="Germany", listing_date="2020-01-01")],
                       sorted(REQUIRED_COLUMNS["companies"]))
    with pytest.raises(ValueError, match="invalid required value"):
        prepare_companies(raw)
