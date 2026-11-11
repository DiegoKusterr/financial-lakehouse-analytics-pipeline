from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from jinja2 import Environment, StrictUndefined

MODEL_ROOT = Path(__file__).resolve().parents[1] / "dbt_finance" / "models"


def model_sql(path):
    environment = Environment(undefined=StrictUndefined)
    environment.globals.update(config=lambda **kwargs: "", ref=lambda name: name)
    return environment.from_string((MODEL_ROOT / path).read_text()).render()


def feature(quarter_date, growth, available):
    row = dict(company_quarter_key=f"1-{quarter_date}", company_id=1, ticker="TEST", sector="Technology",
               industry="Software", country="Germany", fiscal_quarter="fixture",
               fiscal_quarter_start_date=date.fromisoformat(quarter_date),
               feature_available_date=date.fromisoformat(available), revenue_growth_qoq=growth)
    row.update({c: 1.0 for c in ["gross_margin", "operating_margin", "net_margin", "debt_to_equity",
        "return_on_assets", "cash_flow_margin", "asset_turnover", "average_daily_return", "average_volume",
        "average_moving_average_20d", "average_rolling_volatility_20d", "quarter_end_close_price"]})
    return row


def run_ml(rows):
    with duckdb.connect(":memory:") as connection:
        connection.register("mart_company_quarterly_metrics", pd.DataFrame(rows))
        return connection.execute(model_sql("marts/mart_ml_ready_features.sql")
                                  + " order by fiscal_quarter_start_date").fetchdf().to_dict("records")


@pytest.mark.parametrize("growth,flag", [(0.2, 1), (-0.1, 0), (0.0, 0)])
def test_known_labels_and_unknown_terminal_labels(growth, flag):
    first, last = run_ml([feature("2024-01-01", None, "2024-05-15"),
                          feature("2024-04-01", growth, "2024-08-14")])
    assert first["next_quarter_growth_flag"] == flag
    assert first["next_quarter_revenue_growth_qoq"] == pytest.approx(growth)
    assert first["is_training_eligible"]
    assert pd.isna(last["next_quarter_growth_flag"])
    assert pd.isna(last["next_quarter_revenue_growth_qoq"])
    assert pd.isna(last["next_quarter_outcome_available_date"])
    assert not last["is_training_eligible"]


def test_missing_intermediate_quarter_has_no_future_label():
    first, _ = run_ml([feature("2024-01-01", 0.1, "2024-05-15"),
                       feature("2024-07-01", 0.2, "2024-11-14")])
    assert pd.isna(first["next_quarter_growth_flag"])
    assert pd.isna(first["next_quarter_revenue_growth_qoq"])
    assert not first["is_training_eligible"]


def test_consecutive_but_unavailable_growth_is_not_a_negative_label():
    first, _ = run_ml([feature("2024-01-01", None, "2024-05-15"),
                       feature("2024-04-01", None, "2024-08-14")])
    assert pd.isna(first["next_quarter_growth_flag"])
    assert not first["is_training_eligible"]


def test_outcome_already_known_at_feature_date_is_ineligible():
    first, _ = run_ml([feature("2024-01-01", 0.1, "2024-09-01"),
                       feature("2024-04-01", 0.2, "2024-09-01")])
    assert first["next_quarter_growth_flag"] == 1
    assert not first["is_training_eligible"]


def test_ratio_sql_preserves_missing_and_handles_real_zero_denominators():
    row = feature("2024-01-01", None, "2024-05-15")
    row.update(revenue=0.0, gross_profit=None, operating_income=0.0, net_income=0.0,
               total_assets=0.0, total_liabilities=0.0, equity=0.0, operating_cash_flow=None,
               previous_quarter_revenue=None, available_date=date(2024, 5, 15), company_name="Test")
    with duckdb.connect(":memory:") as connection:
        connection.register("stg_quarterly_fundamentals", pd.DataFrame([row]))
        result = connection.execute(model_sql("intermediate/int_fundamental_ratios.sql")).fetchdf().iloc[0]
    assert result.revenue == 0
    assert pd.isna(result.gross_profit)
    for field in ["gross_margin", "net_margin", "debt_to_equity", "return_on_assets", "cash_flow_margin", "asset_turnover"]:
        assert pd.isna(result[field])
