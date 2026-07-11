from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
RNG = np.random.default_rng(seed=20260709)


@dataclass(frozen=True)
class SectorProfile:
    sector: str
    industries: tuple[str, ...]
    name_suffixes: tuple[str, ...]
    drift: float
    volatility: float
    revenue_floor: int
    revenue_ceiling: int
    gross_margin_range: tuple[float, float]
    operating_margin_range: tuple[float, float]
    net_margin_range: tuple[float, float]
    asset_turnover_range: tuple[float, float]
    leverage_range: tuple[float, float]


SECTOR_PROFILES: tuple[SectorProfile, ...] = (
    SectorProfile(
        sector="Technology",
        industries=("Software", "Semiconductors", "Cloud Infrastructure", "IT Services"),
        name_suffixes=("Systems", "Labs", "Platforms", "Networks"),
        drift=0.18,
        volatility=0.34,
        revenue_floor=900_000_000,
        revenue_ceiling=8_500_000_000,
        gross_margin_range=(0.48, 0.72),
        operating_margin_range=(0.10, 0.28),
        net_margin_range=(0.07, 0.22),
        asset_turnover_range=(0.45, 0.95),
        leverage_range=(0.35, 1.05),
    ),
    SectorProfile(
        sector="Healthcare",
        industries=("Medical Devices", "Biotechnology", "Diagnostics", "Managed Care"),
        name_suffixes=("Therapeutics", "Care", "Health", "Meditech"),
        drift=0.12,
        volatility=0.26,
        revenue_floor=700_000_000,
        revenue_ceiling=6_200_000_000,
        gross_margin_range=(0.38, 0.66),
        operating_margin_range=(0.08, 0.24),
        net_margin_range=(0.05, 0.18),
        asset_turnover_range=(0.35, 0.80),
        leverage_range=(0.40, 1.20),
    ),
    SectorProfile(
        sector="Industrials",
        industries=("Machinery", "Aerospace Components", "Logistics Equipment", "Automation"),
        name_suffixes=("Industrial", "Works", "Motion", "Dynamics"),
        drift=0.09,
        volatility=0.22,
        revenue_floor=850_000_000,
        revenue_ceiling=7_500_000_000,
        gross_margin_range=(0.24, 0.44),
        operating_margin_range=(0.06, 0.18),
        net_margin_range=(0.04, 0.12),
        asset_turnover_range=(0.55, 1.10),
        leverage_range=(0.55, 1.60),
    ),
    SectorProfile(
        sector="Consumer Defensive",
        industries=("Packaged Foods", "Household Products", "Grocery Distribution", "Beverages"),
        name_suffixes=("Foods", "Home", "Brands", "Essentials"),
        drift=0.07,
        volatility=0.18,
        revenue_floor=1_000_000_000,
        revenue_ceiling=9_000_000_000,
        gross_margin_range=(0.22, 0.42),
        operating_margin_range=(0.05, 0.15),
        net_margin_range=(0.03, 0.10),
        asset_turnover_range=(0.70, 1.35),
        leverage_range=(0.65, 1.70),
    ),
    SectorProfile(
        sector="Financial Services",
        industries=("Asset Management", "Regional Banking", "Insurance Services", "Brokerage"),
        name_suffixes=("Capital", "Holdings", "Financial", "Partners"),
        drift=0.10,
        volatility=0.24,
        revenue_floor=950_000_000,
        revenue_ceiling=7_800_000_000,
        gross_margin_range=(0.40, 0.68),
        operating_margin_range=(0.12, 0.30),
        net_margin_range=(0.08, 0.20),
        asset_turnover_range=(0.08, 0.20),
        leverage_range=(2.20, 5.40),
    ),
    SectorProfile(
        sector="Energy",
        industries=("Oilfield Services", "Refining", "Exploration", "Midstream Operations"),
        name_suffixes=("Energy", "Petro", "Resources", "Fuel"),
        drift=0.08,
        volatility=0.31,
        revenue_floor=1_100_000_000,
        revenue_ceiling=10_000_000_000,
        gross_margin_range=(0.18, 0.34),
        operating_margin_range=(0.04, 0.16),
        net_margin_range=(0.02, 0.11),
        asset_turnover_range=(0.45, 1.00),
        leverage_range=(0.70, 2.10),
    ),
)

NAME_PREFIXES = (
    "Aster",
    "BlueRiver",
    "NorthPeak",
    "Cedar",
    "Granite",
    "Meridian",
    "Harbor",
    "Summit",
    "Lumen",
    "Atlas",
    "Pioneer",
    "SilverLine",
    "Everfield",
    "IronBridge",
    "Crown",
    "Oakmont",
    "Redwood",
    "Clearwater",
    "Vantage",
    "Stonegate",
)
COUNTRIES = ("United States", "Canada", "United Kingdom", "Germany", "Japan", "Australia")
SECTOR_CASE_VARIANTS = {
    "Technology": ("Technology", "technology", "TECHNOLOGY", " Technology "),
    "Healthcare": ("Healthcare", "healthcare", "HEALTHCARE"),
    "Industrials": ("Industrials", "industrials", "INDUSTRIALS"),
    "Consumer Defensive": ("Consumer Defensive", "consumer defensive", "CONSUMER DEFENSIVE"),
    "Financial Services": ("Financial Services", "financial services", "FINANCIAL SERVICES"),
    "Energy": ("Energy", "energy", "ENERGY", " Energy "),
}


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def random_dates(start: str, end: str, size: int) -> pd.Series:
    start_ts = pd.Timestamp(start).value // 10**9
    end_ts = pd.Timestamp(end).value // 10**9
    random_seconds = RNG.integers(start_ts, end_ts, size=size)
    return pd.to_datetime(random_seconds, unit="s").normalize()


def build_tickers(names: Iterable[str]) -> list[str]:
    tickers: list[str] = []
    used: set[str] = set()

    for index, name in enumerate(names, start=1):
        letters = "".join(character for character in name.upper() if character.isalpha())
        base = letters[:4].ljust(4, "X")
        candidate = base
        suffix = 0

        while candidate in used:
            suffix += 1
            candidate = f"{base[:3]}{suffix}"

        used.add(candidate)
        tickers.append(candidate[:5])

    return tickers


def build_companies(company_count: int = 50) -> pd.DataFrame:
    sector_cycle = np.resize(np.array(SECTOR_PROFILES, dtype=object), company_count)
    RNG.shuffle(sector_cycle)

    company_names: list[str] = []
    profiles: list[SectorProfile] = []

    for index, profile in enumerate(sector_cycle):
        prefix = NAME_PREFIXES[index % len(NAME_PREFIXES)]
        suffix = RNG.choice(profile.name_suffixes)
        company_names.append(f"{prefix} {suffix}")
        profiles.append(profile)

    tickers = build_tickers(company_names)
    listing_dates = random_dates("2005-01-01", "2023-12-31", company_count)

    companies = pd.DataFrame(
        {
            "company_id": np.arange(1001, 1001 + company_count),
            "ticker": tickers,
            "company_name": company_names,
            "sector": [RNG.choice(SECTOR_CASE_VARIANTS[profile.sector]) for profile in profiles],
            "industry": [RNG.choice(profile.industries) for profile in profiles],
            "country": RNG.choice(COUNTRIES, size=company_count),
            "listing_date": listing_dates.strftime("%Y-%m-%d"),
        }
    )

    companies.loc[companies.sample(2, random_state=11).index, "industry"] = None
    companies.loc[companies.sample(1, random_state=13).index, "country"] = None

    duplicate_rows = companies.sample(3, random_state=7)
    companies = pd.concat([companies, duplicate_rows], ignore_index=True)
    return companies


def build_daily_prices(companies: pd.DataFrame) -> pd.DataFrame:
    trading_days = pd.bdate_range("2024-01-02", "2025-12-31", freq="B")
    clean_sector_lookup = {
        company_id: next(
            profile for profile in SECTOR_PROFILES if profile.sector.lower() in sector.lower()
        )
        for company_id, sector in companies.drop_duplicates("company_id")[["company_id", "sector"]].itertuples(index=False)
    }

    records: list[pd.DataFrame] = []
    unique_companies = companies.drop_duplicates("company_id")

    for row in unique_companies.itertuples(index=False):
        profile = clean_sector_lookup[row.company_id]
        base_price = RNG.uniform(18, 240)
        annual_drift = profile.drift
        annual_volatility = profile.volatility

        daily_shocks = RNG.normal(
            loc=annual_drift / 252,
            scale=annual_volatility / np.sqrt(252),
            size=len(trading_days),
        )
        close_prices = base_price * np.exp(np.cumsum(daily_shocks))
        close_prices = np.maximum(close_prices, 2.0)

        base_volume = RNG.integers(180_000, 3_500_000)
        volumes = RNG.lognormal(mean=np.log(base_volume), sigma=0.35, size=len(trading_days))
        volumes = volumes.astype(int)

        company_prices = pd.DataFrame(
            {
                "company_id": row.company_id,
                "date": trading_days.strftime("%Y-%m-%d"),
                "close_price": np.round(close_prices, 2),
                "volume": volumes,
            }
        )
        records.append(company_prices)

    daily_prices = pd.concat(records, ignore_index=True)

    missing_close_idx = daily_prices.sample(frac=0.003, random_state=21).index
    missing_volume_idx = daily_prices.sample(frac=0.003, random_state=22).index
    daily_prices.loc[missing_close_idx, "close_price"] = np.nan
    daily_prices.loc[missing_volume_idx, "volume"] = np.nan

    duplicate_rows = daily_prices.sample(120, random_state=23)
    daily_prices = pd.concat([daily_prices, duplicate_rows], ignore_index=True)
    return daily_prices


def build_quarterly_fundamentals(companies: pd.DataFrame) -> pd.DataFrame:
    quarter_starts = pd.period_range("2024Q1", "2025Q4", freq="Q")
    unique_companies = companies.drop_duplicates("company_id")

    clean_sector_lookup = {
        company_id: next(
            profile for profile in SECTOR_PROFILES if profile.sector.lower() in sector.lower()
        )
        for company_id, sector in unique_companies[["company_id", "sector"]].itertuples(index=False)
    }

    records: list[dict[str, object]] = []

    for row in unique_companies.itertuples(index=False):
        profile = clean_sector_lookup[row.company_id]
        revenue = RNG.integers(profile.revenue_floor, profile.revenue_ceiling)

        for quarter in quarter_starts:
            revenue_growth = RNG.normal(loc=0.02, scale=0.05)
            seasonality = 1 + (0.03 if quarter.quarter == 4 else -0.01 if quarter.quarter == 1 else 0.0)
            revenue = max(revenue * (1 + revenue_growth) * seasonality, profile.revenue_floor * 0.55)

            gross_margin = RNG.uniform(*profile.gross_margin_range)
            operating_margin = min(RNG.uniform(*profile.operating_margin_range), gross_margin - 0.02)
            net_margin = min(RNG.uniform(*profile.net_margin_range), operating_margin - 0.01)
            asset_turnover = RNG.uniform(*profile.asset_turnover_range)
            leverage_ratio = RNG.uniform(*profile.leverage_range)

            total_assets = revenue / asset_turnover
            equity = total_assets / (1 + leverage_ratio)
            total_liabilities = total_assets - equity
            gross_profit = revenue * gross_margin
            operating_income = revenue * operating_margin
            net_income = revenue * net_margin
            operating_cash_flow = net_income * RNG.uniform(1.05, 1.45)

            records.append(
                {
                    "company_id": row.company_id,
                    "fiscal_quarter": f"{quarter.year}-Q{quarter.quarter}",
                    "revenue": round(revenue, 2),
                    "gross_profit": round(gross_profit, 2),
                    "operating_income": round(operating_income, 2),
                    "net_income": round(net_income, 2),
                    "total_assets": round(total_assets, 2),
                    "total_liabilities": round(total_liabilities, 2),
                    "equity": round(equity, 2),
                    "operating_cash_flow": round(operating_cash_flow, 2),
                }
            )

    fundamentals = pd.DataFrame.from_records(records)
    fundamentals.loc[fundamentals.sample(6, random_state=31).index, "operating_cash_flow"] = np.nan
    fundamentals.loc[fundamentals.sample(4, random_state=32).index, "gross_profit"] = np.nan

    duplicate_rows = fundamentals.sample(12, random_state=33)
    fundamentals = pd.concat([fundamentals, duplicate_rows], ignore_index=True)
    return fundamentals


def write_csvs(companies: pd.DataFrame, daily_prices: pd.DataFrame, fundamentals: pd.DataFrame) -> None:
    companies.to_csv(RAW_DIR / "companies.csv", index=False)
    daily_prices.to_csv(RAW_DIR / "daily_prices.csv", index=False)
    fundamentals.to_csv(RAW_DIR / "quarterly_fundamentals.csv", index=False)


def main() -> None:
    ensure_directories()

    companies = build_companies(company_count=50)
    daily_prices = build_daily_prices(companies)
    fundamentals = build_quarterly_fundamentals(companies)
    write_csvs(companies, daily_prices, fundamentals)

    print("Synthetic financial raw data generated successfully.")
    print(f"Companies rows: {len(companies):,}")
    print(f"Daily price rows: {len(daily_prices):,}")
    print(f"Quarterly fundamentals rows: {len(fundamentals):,}")
    print(f"Output directory: {RAW_DIR}")


if __name__ == "__main__":
    main()
