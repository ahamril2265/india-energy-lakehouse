from functools import reduce
from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEMAND_MIN_MW, DEMAND_MAX_MW = 50_000, 270_000
SOURCES = ["thermal_mw", "hydro_mw", "gas_mw", "nuclear_mw", "wind_mw"]
MEASURES = SOURCES + ["solar_mw", "demand_mw", "total_generation_mw"]

# Value-level checks: a failing value is set to null; the rest of the reading is kept
VALUE_CHECKS = {
    **{c: f"{c} >= 0" for c in SOURCES},
    "demand_mw": f"demand_mw BETWEEN {DEMAND_MIN_MW} AND {DEMAND_MAX_MW}",
}

# Delta tables need this feature enabled to store TIMESTAMP_NTZ columns
NTZ = {"delta.feature.timestampNtz": "supported"}


@dp.temporary_view(name="grid_typed")
def grid_typed():
    ts = (F.when(F.col("source_format") == "scada",
                 F.expr("try_to_timestamp(ts_raw, 'yyyy-MM-dd HH:mm:ss')"))
           .otherwise(F.expr("try_to_timestamp(ts_raw, 'dd-MM-yyyy HH:mm:ss')")))
    return spark.read.table("energy.bronze.grid_readings").select(
        ts.cast("timestamp_ntz").alias("ts"),          # IST wall-clock, no time zone
        *[F.expr(f"try_cast({c} AS DOUBLE)").alias(c) for c in MEASURES],
        F.expr("try_cast(resolution_seconds AS INT)").alias("resolution_seconds"),
        "source_format", "source_file", "ts_raw",
    )


@dp.materialized_view(name="energy.ops.grid_quarantine",
                      comment="Grid rows rejected entirely (no usable timestamp)",
                      table_properties=NTZ)
def grid_quarantine():
    return (spark.read.table("grid_typed")
            .where("ts IS NULL")
            .withColumn("failed_rules", F.array(F.lit("valid_timestamp"))))


@dp.materialized_view(name="energy.ops.grid_value_issues",
                      comment="Individual values nulled in silver because they failed a physical check",
                      table_properties=NTZ)
def grid_value_issues():
    typed = spark.read.table("grid_typed").where("ts IS NOT NULL")
    issues = [typed.where(f"{col} IS NOT NULL AND NOT ({rule})")
                   .select("ts", F.lit(col).alias("measure"), F.col(col).alias("raw_value"),
                           F.lit(rule).alias("rule"), "source_format", "source_file")
              for col, rule in VALUE_CHECKS.items()]
    return reduce(lambda a, b: a.unionByName(b), issues)


@dp.temporary_view(name="grid_valid")
@dp.expect_or_drop("valid_timestamp", "ts IS NOT NULL")
def grid_valid():
    df = spark.read.table("grid_typed")
    for col, rule in VALUE_CHECKS.items():
        df = df.withColumn(col, F.when(F.expr(rule), F.col(col)))   # fails or null -> null
    return df


@dp.materialized_view(name="energy.silver.grid_hourly",
                      comment="National grid, hourly average MW per source (= MWh per hour), with completeness",
                      table_properties=NTZ)
def grid_hourly():
    solar = F.col("solar_mw")
    solar_clipped = F.when(solar < 0, F.lit(0.0)).otherwise(solar)   # nulls stay null

    return (spark.read.table("grid_valid")
            .withColumn("hour", F.date_trunc("hour", "ts").cast("timestamp_ntz"))
            .groupBy("hour")
            .agg(F.first("source_format").alias("source_format"),
                 F.max("resolution_seconds").alias("resolution_seconds"),
                 F.count("*").alias("readings"),
                 F.count("demand_mw").alias("demand_readings"),
                 *[F.round(F.avg(c), 2).alias(c) for c in SOURCES],
                 F.round(F.avg(solar_clipped), 2).alias("solar_mw"),
                 F.sum(F.when(solar < 0, 1).otherwise(0)).alias("solar_negative_readings"),
                 F.round(F.avg("demand_mw"), 2).alias("demand_mw"),
                 F.round(F.avg("total_generation_mw"), 2).alias("total_generation_mw"))
            .withColumn("expected_readings", (F.lit(3600) / F.col("resolution_seconds")).cast("int"))
            .withColumn("completeness_pct",
                        F.round(F.col("readings") / F.col("expected_readings") * 100, 1))
            .withColumn("demand_completeness_pct",
                        F.round(F.col("demand_readings") / F.col("expected_readings") * 100, 1)))