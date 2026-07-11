{{ config(materialized='external', location='data/gold/mart_sector_financial_health.parquet') }}

select
    sector || '-' || cast(fiscal_quarter_start_date as varchar) as sector_quarter_key,
    sector,
    fiscal_quarter,
    fiscal_quarter_start_date,
    count(distinct company_id) as company_count,
    avg(revenue_growth_qoq) as average_revenue_growth_qoq,
    avg(gross_margin) as average_gross_margin,
    avg(operating_margin) as average_operating_margin,
    avg(net_margin) as average_net_margin,
    avg(debt_to_equity) as average_debt_to_equity,
    avg(return_on_assets) as average_return_on_assets,
    avg(average_daily_return) as average_daily_return,
    avg(average_rolling_volatility_20d) as average_price_volatility
from {{ ref('mart_company_quarterly_metrics') }}
group by 1, 2, 3, 4
