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
        feature_available_date,
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
),

outcomes as (
    select
        *,
        lead(fiscal_quarter_start_date) over company_quarters as next_observed_quarter,
        lead(revenue_growth_qoq) over company_quarters as next_observed_growth,
        lead(feature_available_date) over company_quarters as next_observed_available_date
    from base
    window company_quarters as (partition by company_id order by fiscal_quarter_start_date)
),

labeled as (
    select
        * exclude (next_observed_quarter, next_observed_growth, next_observed_available_date),
        case when next_observed_quarter = fiscal_quarter_start_date + interval '3 months'
            then next_observed_growth end as next_quarter_revenue_growth_qoq,
        case when next_observed_quarter = fiscal_quarter_start_date + interval '3 months'
             and next_observed_growth is not null
            then next_observed_available_date end as next_quarter_outcome_available_date
    from outcomes
)

select
    *,
    case
        when next_quarter_revenue_growth_qoq is null then null
        when next_quarter_revenue_growth_qoq > 0 then 1
        else 0
    end as next_quarter_growth_flag,
    next_quarter_revenue_growth_qoq is not null
        and next_quarter_outcome_available_date > feature_available_date as is_training_eligible
from labeled
