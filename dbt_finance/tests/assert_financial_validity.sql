select daily_price_key as record_key, 'invalid price/volume' as reason
from {{ ref('stg_daily_prices') }}
where close_price is null or not isfinite(close_price) or close_price <= 0
   or (volume is not null and (not isfinite(volume) or volume < 0))

{% for column in ['revenue', 'gross_profit', 'operating_income', 'net_income',
                  'total_assets', 'total_liabilities', 'equity', 'operating_cash_flow'] %}
union all
select company_quarter_key, 'non-finite {{ column }}'
from {{ ref('stg_quarterly_fundamentals') }}
where {{ column }} is not null and not isfinite({{ column }})
{% endfor %}

union all
select company_quarter_key, 'negative assets/liabilities'
from {{ ref('stg_quarterly_fundamentals') }}
where total_assets < 0 or total_liabilities < 0

{% for column in ['revenue_growth_qoq', 'gross_margin', 'operating_margin', 'net_margin',
                  'debt_to_equity', 'return_on_assets', 'cash_flow_margin', 'asset_turnover',
                  'liabilities_to_assets'] %}
union all
select company_quarter_key, 'non-finite {{ column }}'
from {{ ref('mart_company_quarterly_metrics') }}
where {{ column }} is not null and not isfinite({{ column }})
{% endfor %}
