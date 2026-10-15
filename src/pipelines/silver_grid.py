from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEMAND_MIN_MW, DEMAND_MAX_MW = 50_000, 270_000
SOURCES = ["thermal_mw", "hydro_mw", "gas_mw", "nuclear_mw", "wind_mw"]
MEASURES = SOURCES + ["solar_mw", "demand_mw", "total_generation_mw"]

RULES = {
    "valid_timestamp": "ts IS NOT NULL",
    "demand_in_range": f"demand_mw IS NULL OR demand_mw BETWEEN {DEMAND_MIN_MW} AND {DEMAND_MAX_MW}",
    "generation_non_negative": " AND ".join(f"coalesce({c}, 0) >= 0" for c in SOURCES),
}
ALL_PASS = " AND ".join(f"({r})" for r in RULES.values())


@dp.temporary_view(name="grid_typed")
def grid_typed():
    ts = (F.when(F.col("source_format") == "scada",
                 F.expr("try_to_timestamp(ts_raw, 'yyyy-MM-dd HH:mm:ss')"))
           .otherwise(F.expr("try_to_timestamp(ts_raw, 'dd-MM-yyyy HH:mm:ss')")))
    return spark.read.table("energy.bronze.grid_readings").select(
        ts.alias("ts"),
        *[F.expr(f"try_cast({c} AS DOUBLE)").alias(c) for c in MEASURES],
        F.expr("try_cast(resolution_seconds AS INT)").alias("resolution_seconds"),
        "source_format", "source_file", "ts_raw",
    )


@dp.materialized_view(name="energy.ops.grid_quarantine",
                      comment="Grid readings that failed a silver rule, with the reasons")
def grid_quarantine():
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
        for name, rule in RULES.items()]))
    return (spark.read.table("grid_typed")
            .where(f"NOT coalesce({ALL_PASS}, false)")
            .withColumn("failed_rules", failed))


@dp.temporary_view(name="grid_valid")
@dp.expect_all_or_drop(RULES)
def grid_valid():
    return spark.read.table("grid_typed")


@dp.materialized_view(name="energy.silver.grid_hourly",
                      comment="National grid, hourly average MW per source (= MWh per hour), with completeness")
def grid_hourly():
    solar = F.col("solar_mw")
    solar_clipped = F.when(solar < 0, F.lit(0.0)).otherwise(solar)   # nulls stay null

    return (spark.read.table("grid_valid")
            .withColumn("hour", F.date_trunc("hour", "ts"))
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
                        F.round(F.col("readings") / F.col("expected_readings") * 100, 1)))