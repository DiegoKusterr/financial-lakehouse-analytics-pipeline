from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
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
    shutil.copy2(winutils_path, temporary_bin_dir / "winutils.exe")

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


def build_spark_session() -> SparkSession:
    configure_local_hadoop_home()
    spark_builder = (
        SparkSession.builder.appName("financial-lakehouse-analytics-pipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
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
            F.to_date("listing_date").alias("listing_date"),
        )
        .dropDuplicates(["company_id"])
        .fillna({"industry": "Unknown Industry", "country": "Unknown"})
        .filter(F.col("company_id").isNotNull())
    )

    invalid_sectors = companies.filter(~F.lower(F.col("sector")).isin(*VALID_SECTORS.keys())).count()
    if invalid_sectors:
        raise ValueError("Companies dataset contains sector values outside the expected controlled list.")

    return companies


def prepare_daily_prices(raw_daily_prices: DataFrame, companies: DataFrame) -> DataFrame:
    validate_required_columns(raw_daily_prices, "daily_prices")

    casted = (
        raw_daily_prices.select(
            F.col("company_id").cast("int").alias("company_id"),
            F.to_date("date").alias("date"),
            F.col("close_price").cast("double").alias("close_price"),
            F.col("volume").cast("double").alias("volume"),
        )
        .dropDuplicates(["company_id", "date"])
        .filter(F.col("company_id").isNotNull() & F.col("date").isNotNull())
    )

    company_window = Window.partitionBy("company_id").orderBy("date")
    previous_rows = company_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    next_rows = company_window.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    company_partition = Window.partitionBy("company_id")

    cleaned = (
        casted.withColumn("close_price_forward_fill", F.last("close_price", ignorenulls=True).over(previous_rows))
        .withColumn("close_price_backward_fill", F.first("close_price", ignorenulls=True).over(next_rows))
        .withColumn(
            "close_price",
            F.coalesce("close_price", "close_price_forward_fill", "close_price_backward_fill"),
        )
        .withColumn("company_average_volume", F.avg("volume").over(company_partition))
        .withColumn("volume", F.coalesce("volume", "company_average_volume"))
        .drop("close_price_forward_fill", "close_price_backward_fill", "company_average_volume")
    )

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
            F.round("close_price", 4).alias("close_price"),
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

    casted = (
        raw_fundamentals.select(
            F.col("company_id").cast("int").alias("company_id"),
            F.trim(F.col("fiscal_quarter")).alias("fiscal_quarter"),
            *[F.col(column).cast("double").alias(column) for column in numeric_columns],
        )
        .dropDuplicates(["company_id", "fiscal_quarter"])
        .filter(F.col("company_id").isNotNull() & F.col("fiscal_quarter").isNotNull())
    )

    company_partition = Window.partitionBy("company_id")

    prepared = casted
    for column in numeric_columns:
        prepared = prepared.withColumn(
            column,
            F.coalesce(F.col(column), F.avg(column).over(company_partition), F.lit(0.0)),
        )

    fiscal_year = F.regexp_extract("fiscal_quarter", r"(\d{4})", 1).cast("int")
    quarter_number = F.regexp_extract("fiscal_quarter", r"Q([1-4])", 1).cast("int")

    ordered_window = Window.partitionBy("company_id").orderBy("fiscal_quarter_start_date")

    featured = (
        prepared.withColumn(
            "fiscal_quarter_start_date",
            F.make_date(fiscal_year, ((quarter_number - 1) * 3) + 1, F.lit(1)),
        )
        .withColumn("previous_quarter_revenue", F.lag("revenue").over(ordered_window))
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
            *[F.round(column, 4).alias(column) for column in numeric_columns],
            F.round("previous_quarter_revenue", 4).alias("previous_quarter_revenue"),
            F.round("revenue_growth_qoq", 8).alias("revenue_growth_qoq"),
        )
    )

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
