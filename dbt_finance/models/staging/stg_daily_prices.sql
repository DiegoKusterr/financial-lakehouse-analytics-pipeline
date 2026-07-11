select
    cast(company_id as varchar) || '-' || cast(date as varchar) as daily_price_key,
    cast(company_id as integer) as company_id,
    cast(date as date) as price_date,
    cast(close_price as double) as close_price,
    cast(volume as bigint) as volume,
    cast(daily_return as double) as daily_return,
    cast(moving_average_20d as double) as moving_average_20d,
    cast(rolling_volatility_20d as double) as rolling_volatility_20d,
    ticker,
    company_name,
    sector,
    industry,
    country
from {{ source('silver', 'daily_price_features') }}
