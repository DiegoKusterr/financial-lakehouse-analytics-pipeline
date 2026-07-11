select
    cast(company_id as integer) as company_id,
    upper(trim(ticker)) as ticker,
    company_name,
    sector,
    industry,
    country,
    cast(listing_date as date) as listing_date
from {{ source('silver', 'companies_clean') }}
