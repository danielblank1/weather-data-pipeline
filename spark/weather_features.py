#!/usr/bin/env python3
"""
Spark batch job: Compute advanced weather prediction features.

Reads from PostgreSQL marts, computes heavy features that benefit from
distributed processing (anomaly detection, station clustering, historical
normals), and writes results back to PostgreSQL.

Usage:
    spark-submit --master spark://spark-master:7077 weather_features.py
    spark-submit --master spark://spark-master:7077 weather_features.py --full-history
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

JDBC_URL = "jdbc:postgresql://postgres:5432/weather_warehouse"
JDBC_PROPS = {
    "user": "weather",
    "password": "weather_pipeline_2026",
    "driver": "org.postgresql.Driver",
}


def get_spark():
    """Initialize Spark session with PostgreSQL JDBC driver."""
    return (
        SparkSession.builder.appName("WeatherFeatureEngineering")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.1",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Feature computations
# ---------------------------------------------------------------------------


def compute_historical_normals(df):
    """
    Compute historical normals: average temperature and precipitation
    for each station + day-of-year across all available years.
    These serve as baseline expectations for anomaly detection.
    """
    normals = df.groupBy("station_id", "day_of_year").agg(
        F.avg("tavg_derived_celsius").alias("hist_normal_temp"),
        F.stddev("tavg_derived_celsius").alias("hist_temp_stddev"),
        F.avg("precipitation_mm").alias("hist_normal_precip"),
        F.count("*").alias("years_of_data"),
    )

    # Join back and compute anomaly score
    df_with_normals = df.join(normals, on=["station_id", "day_of_year"], how="left")

    df_with_normals = df_with_normals.withColumn(
        "temp_anomaly",
        F.when(
            F.col("hist_temp_stddev") > 0,
            (F.col("tavg_derived_celsius") - F.col("hist_normal_temp")) / F.col("hist_temp_stddev"),
        ).otherwise(0.0),
    )

    df_with_normals = df_with_normals.withColumn(
        "is_temp_anomaly",
        F.abs(F.col("temp_anomaly")) > 2.0,  # > 2 std deviations
    )

    return df_with_normals


def compute_consecutive_features(df):
    """
    Compute consecutive-day features:
    - Consecutive days of precipitation
    - Consecutive days above/below temperature thresholds
    - Heat/cold wave indicators
    """
    station_date_window = Window.partitionBy("station_id").orderBy("obs_date")

    # Precipitation streak
    df = df.withColumn(
        "precip_flag",
        F.when(F.col("precipitation_mm") > 0, 1).otherwise(0),
    )

    # Running group ID for consecutive precip days
    df = df.withColumn(
        "precip_group",
        F.sum(F.when(F.col("precip_flag") != F.lag("precip_flag", 1).over(station_date_window), 1).otherwise(0)).over(
            station_date_window
        ),
    )

    # Count within group
    group_window = Window.partitionBy("station_id", "precip_group").orderBy("obs_date")
    df = df.withColumn(
        "consecutive_precip_days",
        F.when(F.col("precip_flag") == 1, F.row_number().over(group_window)).otherwise(0),
    )

    # Heat wave: 3+ consecutive days with tmax > 35°C
    df = df.withColumn(
        "heat_flag",
        F.when(F.col("tmax_celsius") > 35, 1).otherwise(0),
    )
    df = df.withColumn(
        "heat_group",
        F.sum(F.when(F.col("heat_flag") != F.lag("heat_flag", 1).over(station_date_window), 1).otherwise(0)).over(
            station_date_window
        ),
    )
    heat_group_window = Window.partitionBy("station_id", "heat_group").orderBy("obs_date")
    df = df.withColumn(
        "heat_wave_day_count",
        F.when(F.col("heat_flag") == 1, F.row_number().over(heat_group_window)).otherwise(0),
    )
    df = df.withColumn(
        "is_heat_wave",
        F.col("heat_wave_day_count") >= 3,
    )

    # Clean up intermediate columns
    df = df.drop("precip_flag", "precip_group", "heat_flag", "heat_group")

    return df


def compute_seasonal_departure(df):
    """
    Compute how much each observation departs from the station's
    seasonal (monthly) average across all years.
    """
    monthly_avg = df.groupBy("station_id", "obs_month").agg(
        F.avg("tavg_derived_celsius").alias("monthly_avg_temp"),
        F.avg("precipitation_mm").alias("monthly_avg_precip"),
    )

    df = df.join(monthly_avg, on=["station_id", "obs_month"], how="left")

    df = df.withColumn(
        "temp_seasonal_departure",
        F.col("tavg_derived_celsius") - F.col("monthly_avg_temp"),
    )

    df = df.withColumn(
        "precip_seasonal_departure",
        F.col("precipitation_mm") - F.col("monthly_avg_precip"),
    )

    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-history", action="store_true")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Weather Feature Engineering — Spark Batch Job")
    print("=" * 60)

    # Read from the dbt mart
    print("Reading from marts.mart_daily_weather …")
    df = spark.read.jdbc(
        JDBC_URL,
        "marts.mart_daily_weather",
        properties=JDBC_PROPS,
    )

    if not args.full_history:
        # Only process the latest year for incremental runs
        max_year = df.agg(F.max("obs_year")).collect()[0][0]
        print(f"Incremental mode: processing year {max_year}")
        df = df.filter(F.col("obs_year") == max_year)

    record_count = df.count()
    print(f"Processing {record_count:,} records …")

    # Compute features
    print("Computing historical normals and anomalies …")
    df = compute_historical_normals(df)

    print("Computing consecutive-day features …")
    df = compute_consecutive_features(df)

    print("Computing seasonal departures …")
    df = compute_seasonal_departure(df)

    # Select final columns for the feature store
    feature_df = df.select(
        "station_id",
        "obs_date",
        "obs_year",
        "obs_month",
        "day_of_year",
        "tavg_derived_celsius",
        "tmax_celsius",
        "tmin_celsius",
        "temp_range_celsius",
        "precipitation_mm",
        "snowfall_mm",
        "avg_wind_speed_ms",
        "tavg_7d_avg",
        "tavg_30d_avg",
        "precip_7d_avg",
        "precip_30d_total",
        "temp_change_1d",
        "temp_change_7d",
        "hist_normal_temp",
        "temp_anomaly",
        "is_temp_anomaly",
        "consecutive_precip_days",
        "heat_wave_day_count",
        "is_heat_wave",
        "temp_seasonal_departure",
        "precip_seasonal_departure",
    )

    # Write to PostgreSQL
    print("Writing features to marts.weather_prediction_features …")
    write_mode = "overwrite" if args.full_history else "append"

    (
        feature_df.write.jdbc(
            JDBC_URL,
            "marts.weather_prediction_features",
            mode=write_mode,
            properties=JDBC_PROPS,
        )
    )

    final_count = feature_df.count()
    print(f"Wrote {final_count:,} feature records. Done!")
    spark.stop()


if __name__ == "__main__":
    main()
