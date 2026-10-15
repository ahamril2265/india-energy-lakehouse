from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JUMP_THRESHOLD_PCT = 30

STATE_NAMES = {"UP": "Uttar Pradesh", "HP": "Himachal Pradesh", "J&K": "Jammu and Kashmir",
               "MP": "Madhya Pradesh", "DNH": "Dadra and Nagar Haveli", "Pondy": "Puducherry"}
REGION_NAMES = {"NR": "Northern", "WR": "Western", "SR": "Southern",
                "ER": "Eastern", "NER": "North-Eastern"}


def lookup(mapping):
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


STATE_MAP, REGION_MAP = lookup(STATE_NAMES), lookup(REGION_NAMES)

RULES = {
    "valid_state":  "state IS NOT NULL AND state <> ''",
    "valid_region": "region IS NOT NULL",
    "valid_date":   "date IS NOT NULL",
    "valid_usage":  "usage_mu IS NOT NULL AND usage_mu > 0",
}
ALL_PASS = " AND ".join(f"({r})" for r in RULES.values())


@dp.temporary_view(name="state_daily_typed")
def state_daily_typed():
    b = (spark.read.table("energy.bronze.state_daily_raw")
         .withColumn("state_code", F.trim("States"))
         .withColumn("region_code", F.trim("Regions")))
    return b.select(
        "state_code",
        F.coalesce(STATE_MAP[F.col("state_code")], F.col("state_code")).alias("state"),
        "region_code",
        REGION_MAP[F.col("region_code")].alias("region"),
        F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
        F.to_date(F.expr("try_to_timestamp(Dates, 'dd/MM/yyyy HH:mm:ss')")).alias("date"),
        F.expr("try_cast(Usage AS DECIMAL(10,2))").alias("usage_mu"),
        "_landing_file",
    )


@dp.materialized_view(name="energy.ops.state_daily_quarantine",
                      comment="State-day rows that failed a silver rule, with the reasons")
def state_daily_quarantine():
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
        for name, rule in RULES.items()]))
    return (spark.read.table("state_daily_typed")
            .where(f"NOT coalesce({ALL_PASS}, false)")
            .withColumn("failed_rules", failed))


@dp.temporary_view(name="state_daily_valid")
@dp.expect_all_or_drop(RULES)
def state_daily_valid():
    return spark.read.table("state_daily_typed")


@dp.materialized_view(name="energy.ops.conflicting_readings",
                      comment="State-days reported twice with different values (DD-01)")
def conflicting_readings():
    return (spark.read.table("state_daily_valid")
            .groupBy("state", "date")
            .agg(F.count("*").alias("copies"),
                 F.countDistinct("usage_mu").alias("distinct_values"),
                 F.sort_array(F.collect_set("usage_mu")).alias("values_mu"),
                 F.min("usage_mu").alias("min_mu"),
                 F.max("usage_mu").alias("max_mu"))
            .where("distinct_values > 1")
            .withColumn("spread_pct",
                        F.round((F.col("max_mu") - F.col("min_mu")) / F.col("min_mu") * 100, 2)))


@dp.materialized_view(name="energy.silver.state_daily",
                      comment="One validated row per state per day; conflicts excluded, jumps flagged")
def state_daily():
    one_per_day = (spark.read.table("state_daily_valid")
                   .groupBy("state_code", "state", "region_code", "region", "date")
                   .agg(F.countDistinct("usage_mu").alias("_distinct_values"),
                        F.first("usage_mu").alias("usage_mu"),
                        F.count("*").alias("source_rows"),
                        F.first("latitude").alias("latitude"),
                        F.first("longitude").alias("longitude"))
                   .where("_distinct_values = 1")             # excludes conflicts (DD-01)
                   .drop("_distinct_values"))

    w = Window.partitionBy("state").orderBy("date")
    prev_date, prev_usage = F.lag("date").over(w), F.lag("usage_mu").over(w)

    return (one_per_day
            .withColumn("pct_change_vs_prev_day",
                        F.when(F.datediff("date", prev_date) == 1,
                               F.round((F.col("usage_mu") - prev_usage) / prev_usage * 100, 2)))
            .withColumn("suspicious_jump",
                        F.coalesce(F.abs("pct_change_vs_prev_day") > JUMP_THRESHOLD_PCT,
                                   F.lit(False))))