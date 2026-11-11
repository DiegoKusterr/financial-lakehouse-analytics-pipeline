from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(scope="session")
def spark():
    from spark_etl import build_spark_session

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = build_spark_session(master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def companies(spark):
    from spark_etl import prepare_companies

    raw = spark.createDataFrame(
        [("1", " test ", "Test Company", " technology ", "Software", "Germany", "2020-01-01")],
        "company_id string, ticker string, company_name string, sector string, industry string, country string, listing_date string",
    )
    result = prepare_companies(raw).cache()
    result.count()
    return result


def string_frame(spark, records, columns):
    return spark.createDataFrame(
        [tuple(None if row.get(c) is None else str(row[c]) for c in columns) for row in records],
        ", ".join(f"{c} string" for c in columns),
    )
