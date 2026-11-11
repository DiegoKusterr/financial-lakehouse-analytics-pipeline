from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from functools import reduce
from pathlib import Path

import pyspark
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
SILVER_DIR = ROOT_DIR / "data" / "silver"
SPARK_SUPPORT_SOURCE = ROOT_DIR / "src" / "spark_support" / "WindowsSafeRawLocalFileSystem.java"
WINDOWS_WINUTILS_CANDIDATES = (
    Path("C:/Program Files/RStudio/resources/app/bin/winutils.exe"),
    Path("C:/Program Files/RStudio/resources/app/bin/winutils/winutils.exe"),
    Path("C:/Program Files/RStudio/resources/app/bin/winutils/x64/winutils.exe"),
    Path("C:/Program Files/R/RStudio/resources/app/bin/winutils.exe"),
    Path("C:/Program Files/R/RStudio/resources/app/bin/winutils/winutils.exe"),
    Path("C:/Program Files/R/RStudio/resources/app/bin/winutils/x64/winutils.exe"),
)

REQUIRED_COLUMNS = {
    "companies": {
        "company_id",
        "ticker",
        "company_name",
        "sector",
        "industry",
        "country",
        "listing_date",
    },
    "daily_prices": {"company_id", "date", "close_price", "volume"},
    "quarterly_fundamentals": {
        "company_id",
        "fiscal_quarter",
        "available_date",
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "total_assets",
        "total_liabilities",
        "equity",
        "operating_cash_flow",
    },
}

VALID_SECTORS = {
    "technology": "Technology",
    "healthcare": "Healthcare",
    "industrials": "Industrials",
    "consumer defensive": "Consumer Defensive",
    "financial services": "Financial Services",
    "energy": "Energy",
}


def resolve_windows_winutils_path() -> Path | None:
    configured_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if configured_home:
        configured_path = Path(configured_home)
        configured_winutils = configured_path / "bin" / "winutils.exe"
        if configured_winutils.exists():
            return configured_winutils

    winutils_path = shutil.which("winutils.exe")
    if winutils_path:
        return Path(winutils_path).resolve()

    for candidate_path in WINDOWS_WINUTILS_CANDIDATES:
        if candidate_path.exists():
            return candidate_path

    return None


def configure_local_hadoop_home() -> None:
    if os.name != "nt":
        return

    winutils_path = resolve_windows_winutils_path()
    if winutils_path is None:
        raise RuntimeError(
            "Spark native Parquet writes on Windows require winutils.exe. "
            "Set HADOOP_HOME to a valid Hadoop home or install a local environment that provides winutils.exe."
        )

    temporary_hadoop_home = Path(tempfile.gettempdir()) / "financial_lakehouse_hadoop"
    temporary_bin_dir = temporary_hadoop_home / "bin"
    temporary_bin_dir.mkdir(parents=True, exist_ok=True)
    target = temporary_bin_dir / "winutils.exe"
    if winutils_path.resolve() != target.resolve():
        shutil.copy2(winutils_path, target)

    os.environ["HADOOP_HOME"] = str(temporary_hadoop_home)
    os.environ["hadoop.home.dir"] = str(temporary_hadoop_home)


def compile_windows_spark_support() -> Path | None:
    if os.name != "nt":
        return None

    classes_dir = Path(tempfile.gettempdir()) / "financial_lakehouse_spark_support_classes"
    compiled_class = classes_dir / "spark" / "support" / "WindowsSafeRawLocalFileSystem.class"

    if compiled_class.exists() and compiled_class.stat().st_mtime >= SPARK_SUPPORT_SOURCE.stat().st_mtime:
        return classes_dir

    classes_dir.mkdir(parents=True, exist_ok=True)
    jars_dir = Path(pyspark.__file__).resolve().parent / "jars"
    classpath = str(jars_dir / "*")

    try:
        subprocess.run(
            ["javac", "-cp", classpath, "-d", str(classes_dir), str(SPARK_SUPPORT_SOURCE)],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            "Unable to compile the Windows Spark support class required for native Parquet writes.\n"
            f"stdout:\n{error.stdout}\n"
            f"stderr:\n{error.stderr}"
        ) from error

    return classes_dir


def build_spark_session(master: str = "local[*]") -> SparkSession:
    configure_local_hadoop_home()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark_builder = (
        SparkSession.builder.appName("financial-lakehouse-analytics-pipeline")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )

    support_classes_dir = compile_windows_spark_support()
    if support_classes_dir is not None:
        spark_builder = (
            spark_builder.config("spark.driver.extraClassPath", str(support_classes_dir))
            .config("spark.executor.extraClassPath", str(support_classes_dir))
            .config("spark.hadoop.fs.file.impl", "spark.support.WindowsSafeRawLocalFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        )

    return spark_builder.getOrCreate()


def read_raw_csv(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", False).csv(str(path))


def validate_required_columns(df: DataFrame, dataset_name: str) -> None:
    missing_columns = sorted(REQUIRED_COLUMNS[dataset_name] - set(df.columns))
    if missing_columns:
        raise ValueError(f"{dataset_name} is missing required columns: {', '.join(missing_columns)}")


def reject_rows(df: DataFrame, predicate: F.Column, message: str, keys: list[str]) -> None:
    """Fail before writing any outputs; show keys without exposing financial payloads."""
    examples = df.filter(predicate).select(*keys).limit(5).collect()
    if examples:
        raise ValueError(f"{message}; example keys: {[row.asDict() for row in examples]}")


def deduplicate_records(df: DataFrame, keys: list[str], dataset_name: str) -> DataFrame:
    """Exact repeats are harmless; conflicting business keys have no arbitrary winner."""
    distinct = df.dropDuplicates()
    conflicts = distinct.groupBy(*keys).count().filter(F.col("count") > 1)
    reject_rows(conflicts, F.lit(True), f"{dataset_name}: conflicting records", keys)
    return distinct


def validate_numeric_inputs(
    df: DataFrame, columns: list[str], dataset_name: str, keys: list[str],
    positive: tuple[str, ...] = (), nonnegative: tuple[str, ...] = (),
) -> None:
    invalid = []
    for column in columns:
        value = F.expr(f"try_cast(`{column}` as double)")
        supplied = F.col(column).isNotNull() & (F.trim(F.col(column).cast("string")) != "")
        bad = value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
        if column in positive:
            bad = bad | (value <= 0)
        if column in nonnegative:
            bad = bad | (value < 0)
        if column == "volume":
            bad = bad | (value >= float(2**63))
        invalid.append(supplied & bad)
    reject_rows(df, reduce(lambda a, b: a | b, invalid),
                f"{dataset_name}: invalid numeric value in {', '.join(columns)}", keys)


def validate_company_references(df: DataFrame, companies: DataFrame, dataset_name: str) -> None:
    unknown = df.join(companies.select("company_id"), "company_id", "left_anti")
    reject_rows(unknown, F.lit(True), f"{dataset_name}: unknown company_id", ["company_id"])


def normalize_sector(column_name: str) -> F.Column:
    normalized = F.lower(F.trim(F.col(column_name)))
    return (
        F.when(normalized == "technology", F.lit("Technology"))
        .when(normalized == "healthcare", F.lit("Healthcare"))
        .when(normalized == "industrials", F.lit("Industrials"))
        .when(normalized == "consumer defensive", F.lit("Consumer Defensive"))
        .when(normalized == "financial services", F.lit("Financial Services"))
        .when(normalized == "energy", F.lit("Energy"))
        .otherwise(F.initcap(normalized))
    )


def prepare_companies(raw_companies: DataFrame) -> DataFrame:
    validate_required_columns(raw_companies, "companies")

    companies = (
        raw_companies.select(
            F.col("company_id").cast("int").alias("company_id"),
            F.upper(F.trim(F.col("ticker"))).alias("ticker"),
            F.trim(F.col("company_name")).alias("company_name"),
            normalize_sector("sector").alias("sector"),
            F.trim(F.col("industry")).alias("industry"),
            F.trim(F.col("country")).alias("country"),
            F.expr("try_cast(listing_date as date)").alias("listing_date"),
        )
        .fillna({"industry": "Unknown Industry", "country": "Unknown"})
    )
    reject_rows(companies,
                F.col("company_id").isNull() | F.col("sector").isNull()
                | ~F.lower("sector").isin(*VALID_SECTORS)
                | F.col("ticker").isNull() | (F.col("ticker") == "")
                | F.col("company_name").isNull() | (F.col("company_name") == "")
                | F.col("listing_date").isNull(),
                "companies: invalid required value", ["company_id"])
    return deduplicate_records(companies, ["company_id"], "companies")


def prepare_daily_prices(raw_daily_prices: DataFrame, companies: DataFrame) -> DataFrame:
    validate_required_columns(raw_daily_prices, "daily_prices")
    validate_numeric_inputs(raw_daily_prices, ["close_price", "volume"], "daily_prices",
                            ["company_id", "date"], positive=("close_price",), nonnegative=("volume",))

    casted = (
        raw_daily_prices.select(
            F.col("company_id").cast("int").alias("company_id"),
            F.expr("try_cast(date as date)").alias("date"),
            F.col("close_price").cast("double").alias("close_price"),
            F.col("volume").cast("double").alias("volume"),
        )
    )
    reject_rows(casted, F.col("company_id").isNull() | F.col("date").isNull(),
                "daily_prices: invalid company_id/date", ["company_id", "date"])
    casted = deduplicate_records(casted, ["company_id", "date"], "daily_prices")
    validate_company_references(casted, companies, "daily_prices")

    company_window = Window.partitionBy("company_id").orderBy("date")
    previous_rows = company_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    cleaned = (
        casted.withColumn("close_price", F.last("close_price", ignorenulls=True).over(previous_rows))
        .withColumn("volume", F.coalesce("volume", F.avg("volume").over(previous_rows)))
    )
    # A leading missing close cannot satisfy the price contract without future data.
    reject_rows(cleaned, F.col("close_price").isNull(),
                "daily_prices: missing close_price without an observed past price",
                ["company_id", "date"])

    featured = (
        cleaned.withColumn("previous_close_price", F.lag("close_price").over(company_window))
        .withColumn(
            "daily_return",
            F.when(
                F.col("previous_close_price").isNull() | (F.col("previous_close_price") == 0),
                F.lit(None),
            ).otherwise((F.col("close_price") - F.col("previous_close_price")) / F.col("previous_close_price")),
        )
        .withColumn(
            "moving_average_20d",
            F.avg("close_price").over(company_window.rowsBetween(-19, 0)),
        )
        .withColumn(
            "rolling_volatility_20d",
            F.stddev_samp("daily_return").over(company_window.rowsBetween(-19, 0)),
        )
        .join(companies, on="company_id", how="left")
        .select(
            "company_id",
            "ticker",
            "company_name",
            "sector",
            "industry",
            "country",
            "date",
            "close_price",
            F.round("volume", 0).cast("bigint").alias("volume"),
            F.round("daily_return", 8).alias("daily_return"),
            F.round("moving_average_20d", 4).alias("moving_average_20d"),
            F.round("rolling_volatility_20d", 8).alias("rolling_volatility_20d"),
        )
    )

    return featured


def prepare_quarterly_fundamentals(raw_fundamentals: DataFrame, companies: DataFrame) -> DataFrame:
    validate_required_columns(raw_fundamentals, "quarterly_fundamentals")

    numeric_columns = [
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "total_assets",
        "total_liabilities",
        "equity",
        "operating_cash_flow",
    ]
    validate_numeric_inputs(raw_fundamentals, numeric_columns, "quarterly_fundamentals",
                            ["company_id", "fiscal_quarter"],
                            nonnegative=("total_assets", "total_liabilities"))
    reject_rows(raw_fundamentals,
                F.col("fiscal_quarter").isNull()
                | ~F.col("fiscal_quarter").rlike(r"^[0-9]{4}-Q[1-4]$")
                | (F.length("fiscal_quarter") != 7)
                | (F.substring("fiscal_quarter", 1, 4) == "0000"),
                "quarterly_fundamentals: invalid fiscal_quarter (expected YYYY-Q[1-4])",
                ["company_id", "fiscal_quarter"])

    casted = (
        raw_fundamentals.select(
            F.col("company_id").cast("int").alias("company_id"),
            F.trim(F.col("fiscal_quarter")).alias("fiscal_quarter"),
            F.expr("try_cast(available_date as date)").alias("available_date"),
            *[F.col(column).cast("double").alias(column) for column in numeric_columns],
        )
    )
    reject_rows(casted, F.col("company_id").isNull() | F.col("available_date").isNull(),
                "quarterly_fundamentals: invalid company_id/available_date",
                ["company_id", "fiscal_quarter"])
    casted = deduplicate_records(casted, ["company_id", "fiscal_quarter"], "quarterly_fundamentals")
    validate_company_references(casted, companies, "quarterly_fundamentals")
    fiscal_year = F.substring("fiscal_quarter", 1, 4).cast("int")
    quarter_number = F.substring("fiscal_quarter", 7, 1).cast("int")

    ordered_window = Window.partitionBy("company_id").orderBy("fiscal_quarter_start_date")

    featured = (
        casted.withColumn(
            "fiscal_quarter_start_date",
            F.make_date(fiscal_year, ((quarter_number - 1) * 3) + 1, F.lit(1)),
        )
        .withColumn("previous_quarter_start_date", F.lag("fiscal_quarter_start_date").over(ordered_window))
        .withColumn("lagged_revenue", F.lag("revenue").over(ordered_window))
        .withColumn("lagged_available_date", F.lag("available_date").over(ordered_window))
        .withColumn("previous_quarter_revenue", F.when(
            F.col("previous_quarter_start_date") == F.add_months("fiscal_quarter_start_date", -3),
            F.col("lagged_revenue")))
        .withColumn("feature_available_date", F.when(
            F.col("previous_quarter_revenue").isNotNull(),
            F.greatest("available_date", "lagged_available_date")).otherwise(F.col("available_date")))
        .withColumn(
            "revenue_growth_qoq",
            F.when(
                F.col("previous_quarter_revenue").isNull() | (F.col("previous_quarter_revenue") == 0),
                F.lit(None),
            ).otherwise((F.col("revenue") - F.col("previous_quarter_revenue")) / F.col("previous_quarter_revenue")),
        )
        .join(companies, on="company_id", how="left")
        .select(
            "company_id",
            "ticker",
            "company_name",
            "sector",
            "industry",
            "country",
            "fiscal_quarter",
            "fiscal_quarter_start_date",
            "available_date",
            "feature_available_date",
            *[F.round(column, 4).alias(column) for column in numeric_columns],
            F.round("previous_quarter_revenue", 4).alias("previous_quarter_revenue"),
            F.round("revenue_growth_qoq", 8).alias("revenue_growth_qoq"),
        )
    )
    reject_rows(featured,
                F.col("available_date") < F.last_day(F.add_months("fiscal_quarter_start_date", 2)),
                "quarterly_fundamentals: available_date precedes quarter end",
                ["company_id", "fiscal_quarter"])
    return featured


def write_parquet(df: DataFrame, output_path: Path) -> None:
    df.coalesce(1).write.mode("overwrite").parquet(str(output_path))


def main() -> None:
    spark = build_spark_session()

    try:
        raw_companies = read_raw_csv(spark, RAW_DIR / "companies.csv")
        raw_daily_prices = read_raw_csv(spark, RAW_DIR / "daily_prices.csv")
        raw_fundamentals = read_raw_csv(spark, RAW_DIR / "quarterly_fundamentals.csv")

        companies = prepare_companies(raw_companies)
        daily_price_features = prepare_daily_prices(raw_daily_prices, companies)
        quarterly_fundamental_features = prepare_quarterly_fundamentals(raw_fundamentals, companies)

        write_parquet(companies, SILVER_DIR / "companies_clean")
        write_parquet(daily_price_features, SILVER_DIR / "daily_price_features")
        write_parquet(quarterly_fundamental_features, SILVER_DIR / "quarterly_fundamental_features")

        print("PySpark ETL completed successfully.")
        print(f"companies_clean rows: {companies.count():,}")
        print(f"daily_price_features rows: {daily_price_features.count():,}")
        print(f"quarterly_fundamental_features rows: {quarterly_fundamental_features.count():,}")
        print(f"Silver output directory: {SILVER_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
