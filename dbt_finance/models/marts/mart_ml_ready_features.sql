{{ config(materialized='external', location='data/gold/mart_ml_ready_features.parquet') }}

with base as (
    select
        company_quarter_key,
        company_id,
        ticker,
        sector,
        industry,
        country,
        fiscal_quarter,
        fiscal_quarter_start_date,
        revenue_growth_qoq,
        gross_margin,
        operating_margin,
        net_margin,
        debt_to_equity,
        return_on_assets,
        cash_flow_margin,
        asset_turnover,
        average_daily_return,
        average_volume,
        average_moving_average_20d,
        average_rolling_volatility_20d,
        quarter_end_close_price
    from {{ ref('mart_company_quarterly_metrics') }}
)

select
    *,
    lead(revenue_growth_qoq) over (
        partition by company_id
        order by fiscal_quarter_start_date
    ) as next_quarter_revenue_growth_qoq,
    case
        when lead(revenue_growth_qoq) over (
            partition by company_id
            order by fiscal_quarter_start_date
        ) > 0 then 1
        else 0
    end as next_quarter_growth_flag
from base
