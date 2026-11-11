with history as (
    select *,
        lag(fiscal_quarter_start_date) over company_quarters as prior_date,
        lag(revenue) over company_quarters as prior_revenue,
        lag(available_date) over company_quarters as prior_available_date
    from {{ ref('stg_quarterly_fundamentals') }}
    window company_quarters as (partition by company_id order by fiscal_quarter_start_date)
)
select company_quarter_key
from history
where not regexp_full_match(fiscal_quarter, '[0-9]{4}-Q[1-4]')
   or fiscal_quarter_start_date is null
   or fiscal_quarter <> strftime(fiscal_quarter_start_date, '%Y') || '-Q' || quarter(fiscal_quarter_start_date)
   or available_date is null or feature_available_date is null
   or available_date < last_day(fiscal_quarter_start_date + interval '2 months')
   or feature_available_date < available_date
   or (previous_quarter_revenue is not null and
       (prior_date is distinct from fiscal_quarter_start_date - interval '3 months'
        or previous_quarter_revenue is distinct from prior_revenue
        or feature_available_date < prior_available_date))
   or (prior_date is distinct from fiscal_quarter_start_date - interval '3 months'
       and revenue_growth_qoq is not null)
   or (prior_date = fiscal_quarter_start_date - interval '3 months'
       and previous_quarter_revenue is distinct from prior_revenue)
