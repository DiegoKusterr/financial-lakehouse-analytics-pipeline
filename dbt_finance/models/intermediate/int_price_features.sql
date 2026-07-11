with daily_prices as (
    select
        company_id,
        ticker,
        company_name,
        sector,
        industry,
        country,
        price_date,
        close_price,
        volume,
        daily_return,
        moving_average_20d,
        rolling_volatility_20d,
        date_trunc('quarter', price_date) as fiscal_quarter_start_date
    from {{ ref('stg_daily_prices') }}
),

quarterly_aggregates as (
    select
        cast(company_id as varchar) || '-' || cast(fiscal_quarter_start_date as varchar) as company_quarter_feature_key,
        company_id,
        ticker,
        company_name,
        sector,
        industry,
        country,
        fiscal_quarter_start_date,
        avg(close_price) as average_close_price,
        max(close_price) as quarter_high_close_price,
        min(close_price) as quarter_low_close_price,
        avg(daily_return) as average_daily_return,
        avg(volume) as average_volume,
        avg(moving_average_20d) as average_moving_average_20d,
        avg(rolling_volatility_20d) as average_rolling_volatility_20d
    from daily_prices
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

quarter_end_prices as (
    select
        company_id,
        fiscal_quarter_start_date,
        close_price as quarter_end_close_price,
        row_number() over (
            partition by company_id, fiscal_quarter_start_date
            order by price_date desc
        ) as quarter_end_rank
    from daily_prices
)

select
    quarterly_aggregates.company_quarter_feature_key,
    quarterly_aggregates.company_id,
    quarterly_aggregates.ticker,
    quarterly_aggregates.company_name,
    quarterly_aggregates.sector,
    quarterly_aggregates.industry,
    quarterly_aggregates.country,
    quarterly_aggregates.fiscal_quarter_start_date,
    quarterly_aggregates.average_close_price,
    quarterly_aggregates.quarter_high_close_price,
    quarterly_aggregates.quarter_low_close_price,
    quarter_end_prices.quarter_end_close_price,
    quarterly_aggregates.average_daily_return,
    quarterly_aggregates.average_volume,
    quarterly_aggregates.average_moving_average_20d,
    quarterly_aggregates.average_rolling_volatility_20d
from quarterly_aggregates
left join quarter_end_prices
    on quarterly_aggregates.company_id = quarter_end_prices.company_id
   and quarterly_aggregates.fiscal_quarter_start_date = quarter_end_prices.fiscal_quarter_start_date
   and quarter_end_prices.quarter_end_rank = 1
