# Data Dictionary

## Raw Data

### `data/raw/companies.csv`

| Column | Type | Description |
| --- | --- | --- |
| `company_id` | integer | Stable company identifier. |
| `ticker` | string | Synthetic stock ticker. |
| `company_name` | string | Synthetic public company name. |
| `sector` | string | Company sector, intentionally generated with some inconsistent casing. |
| `industry` | string | More specific business classification within a sector. |
| `country` | string | Country where the company is listed. |
| `listing_date` | date string | Synthetic listing date. |

### `data/raw/daily_prices.csv`

| Column | Type | Description |
| --- | --- | --- |
| `company_id` | integer | Stable company identifier. |
| `date` | date string | Trading date. |
| `close_price` | decimal | Synthetic daily closing share price. |
| `volume` | integer | Synthetic daily traded share volume. |

### `data/raw/quarterly_fundamentals.csv`

| Column | Type | Description |
| --- | --- | --- |
| `company_id` | integer | Stable company identifier. |
| `fiscal_quarter` | string | Fiscal quarter in `YYYY-QN` format. |
| `revenue` | decimal | Quarterly revenue. |
| `gross_profit` | decimal | Revenue minus cost of goods or service delivery. |
| `operating_income` | decimal | Earnings from operations before non-operating items. |
| `net_income` | decimal | Bottom-line profit for the quarter. |
| `total_assets` | decimal | Total company assets. |
| `total_liabilities` | decimal | Total company liabilities. |
| `equity` | decimal | Shareholders' equity. |
| `operating_cash_flow` | decimal | Cash generated from operating activity. |

## Silver Data

### `data/silver/companies_clean/`

Spark writes this dataset as a Parquet directory in the silver layer.

| Column | Type | Description |
| --- | --- | --- |
| `company_id` | integer | Deduplicated company identifier. |
| `ticker` | string | Uppercase ticker. |
| `company_name` | string | Clean company name. |
| `sector` | string | Standardized sector value. |
| `industry` | string | Filled industry value. Missing values become `Unknown Industry`. |
| `country` | string | Filled country value. Missing values become `Unknown`. |
| `listing_date` | date | Parsed listing date. |

### `data/silver/daily_price_features/`

Spark writes this dataset as a Parquet directory in the silver layer.

| Column | Type | Description |
| --- | --- | --- |
| `company_id` | integer | Company identifier. |
| `ticker` | string | Uppercase ticker. |
| `company_name` | string | Company name joined from the cleaned company dimension. |
| `sector` | string | Standardized sector joined from the company dimension. |
| `industry` | string | Industry joined from the company dimension. |
| `country` | string | Country joined from the company dimension. |
| `date` | date | Trading date. |
| `close_price` | decimal | Cleaned closing share price. |
| `volume` | integer | Cleaned daily traded volume. |
| `daily_return` | decimal | Daily percentage return based on the previous close. |
| `moving_average_20d` | decimal | Rolling 20-trading-day average close price. |
| `rolling_volatility_20d` | decimal | Rolling 20-trading-day standard deviation of daily returns. |

### `data/silver/quarterly_fundamental_features/`

Spark writes this dataset as a Parquet directory in the silver layer.

| Column | Type | Description |
| --- | --- | --- |
| `company_id` | integer | Company identifier. |
| `ticker` | string | Uppercase ticker. |
| `company_name` | string | Company name joined from the cleaned company dimension. |
| `sector` | string | Standardized sector joined from the company dimension. |
| `industry` | string | Industry joined from the company dimension. |
| `country` | string | Country joined from the company dimension. |
| `fiscal_quarter` | string | Fiscal quarter in `YYYY-QN` format. |
| `fiscal_quarter_start_date` | date | First day of the fiscal quarter, derived in Spark. |
| `revenue` | decimal | Quarterly revenue after missing-value treatment. |
| `gross_profit` | decimal | Quarterly gross profit after missing-value treatment. |
| `operating_income` | decimal | Quarterly operating income after missing-value treatment. |
| `net_income` | decimal | Quarterly net income after missing-value treatment. |
| `total_assets` | decimal | Quarterly assets after missing-value treatment. |
| `total_liabilities` | decimal | Quarterly liabilities after missing-value treatment. |
| `equity` | decimal | Quarterly equity after missing-value treatment. |
| `operating_cash_flow` | decimal | Quarterly operating cash flow after missing-value treatment. |
| `previous_quarter_revenue` | decimal | Revenue from the immediately prior quarter. |
| `revenue_growth_qoq` | decimal | Quarter-over-quarter revenue growth rate. |

## Gold Data

### `mart_company_quarterly_metrics`

Final company-quarter analytics mart combining fundamental metrics, ratio calculations, and aggregated market features.

Key fields:

- `company_quarter_key`
- `company_id`
- `ticker`
- `sector`
- `fiscal_quarter`
- `revenue_growth_qoq`
- `gross_margin`
- `net_margin`
- `debt_to_equity`
- `return_on_assets`
- `average_daily_return`
- `average_rolling_volatility_20d`

### `mart_sector_financial_health`

Sector-quarter summary mart used to compare sector performance and financial health.

Key fields:

- `sector_quarter_key`
- `sector`
- `fiscal_quarter`
- `company_count`
- `average_revenue_growth_qoq`
- `average_gross_margin`
- `average_net_margin`
- `average_debt_to_equity`
- `average_return_on_assets`
- `average_price_volatility`

### `mart_ml_ready_features`

Feature table that keeps company-quarter observations in a modeling-friendly shape without training a machine learning model.

Key fields:

- `company_quarter_key`
- `company_id`
- `sector`
- `fiscal_quarter_start_date`
- `gross_margin`
- `net_margin`
- `debt_to_equity`
- `asset_turnover`
- `average_daily_return`
- `average_volume`
- `average_rolling_volatility_20d`
- `next_quarter_revenue_growth_qoq`
- `next_quarter_growth_flag`
