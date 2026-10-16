from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TOLERANCE_PCT = 10
MIN_COVERAGE_PCT = 50
IMPLAUSIBLE_RATIO = 0.3

# Official name -> (silver name, match quality)
NAME_MAP = {
    "UTs of J&K and Ladakh": ("Jammu and Kashmir", "approximate"),
    "Dadra & Nagar Haveli and Daman & Diu": ("Dadra and Nagar Haveli", "approximate"),
}


def lookup(position):
    return F.create_map(*[F.lit(x) for k, v in NAME_MAP.items() for x in (k, v[position])])


STATE_MAP, MATCH_MAP = lookup(0), lookup(1)


@dp.temporary_view(name="official_typed")
def official_typed():
    name = F.trim("state_name")
    return spark.read.table("energy.bronze.state_monthly_official_raw").select(
        name.alias("official_name"),
        F.coalesce(STATE_MAP[name], name).alias("state"),
        F.coalesce(MATCH_MAP[name], F.lit("exact")).alias("name_match"),
        F.to_date(F.expr("try_to_timestamp(month, 'yyyy-MM-dd')")).alias("month"),
        F.expr("try_cast(energy_requirement AS DECIMAL(12,2))").alias("requirement_mu"),
        F.expr("try_cast(energy_availability AS DECIMAL(12,2))").alias("availability_mu"),
    )


@dp.materialized_view(name="energy.ops.official_conflicts",
                      comment="Official state-months reported more than once with different values")
def official_conflicts():
    return (spark.read.table("official_typed")
            .groupBy("official_name", "month")
            .agg(F.count("*").alias("copies"),
                 F.sort_array(F.collect_set("availability_mu")).alias("values_mu"))
            .where(F.size("values_mu") > 1))


@dp.materialized_view(name="energy.silver.state_monthly_official",
                      comment="Official monthly energy availability per state; deduplicated, plausibility-flagged")
@dp.expect_all_or_drop({
    "valid_month": "month IS NOT NULL",
    "valid_availability": "availability_mu IS NOT NULL AND availability_mu > 0",
})
def state_monthly_official():
    one_per_month = (spark.read.table("official_typed")
                     .groupBy("official_name", "state", "name_match", "month")
                     .agg(F.countDistinct("availability_mu").alias("_distinct"),
                          F.count("*").alias("source_rows"),
                          F.first("availability_mu").alias("availability_mu"),
                          F.first("requirement_mu").alias("requirement_mu"))
                     .where("_distinct = 1")
                     .drop("_distinct"))

    by_state = Window.partitionBy("state")
    in_order = Window.partitionBy("state").orderBy("month")

    # Lower of the previous and next month; least() skips nulls, so one neighbour is enough.
    # An isolated dip is flagged; a sustained shock with gradual recovery is kept (DD-12).
    neighbour_min = F.least(F.lag("availability_mu").over(in_order),
                            F.lead("availability_mu").over(in_order))

    return (one_per_month
            .withColumn("state_median_mu", F.percentile_approx("availability_mu", 0.5).over(by_state))
            .withColumn("neighbour_min_mu", neighbour_min)
            .withColumn("implausible",
                        F.col("availability_mu")
                        < F.coalesce(F.col("neighbour_min_mu"), F.col("state_median_mu"))
                        * IMPLAUSIBLE_RATIO))


@dp.materialized_view(name="energy.silver.state_month_reconciliation",
                      comment="Kaggle daily data rolled up to months and compared with official CEA figures")
def state_month_reconciliation():
    kaggle = (spark.read.table("energy.silver.state_daily")
              .withColumn("month", F.trunc("date", "month"))
              .groupBy("state", "region", "month")
              .agg(F.count("*").alias("valid_days"),
                   F.avg("usage_mu").alias("avg_daily_mu"),
                   F.sum(F.col("suspicious_jump").cast("int")).alias("jump_days"))
              .withColumn("days_in_month", F.dayofmonth(F.last_day("month")))
              .withColumn("coverage_pct",
                          F.round(F.col("valid_days") / F.col("days_in_month") * 100, 1))
              .withColumn("kaggle_est_mu",
                          F.round(F.col("avg_daily_mu") * F.col("days_in_month"), 2)))

    official = (spark.read.table("energy.silver.state_monthly_official")
                .select("state", "month", "official_name", "name_match",
                        F.col("availability_mu").alias("official_mu"), "implausible"))

    diff_pct = F.round((F.col("kaggle_est_mu") - F.col("official_mu")) / F.col("official_mu") * 100, 2)

    return (kaggle.join(official, ["state", "month"], "left")
            .withColumn("diff_pct", diff_pct)
            .withColumn("status",
                        F.when(F.col("official_mu").isNull(), "no_official_data")
                         .when(F.col("implausible"), "official_implausible")
                         .when(F.col("coverage_pct") < MIN_COVERAGE_PCT, "insufficient_coverage")
                         .when(F.abs("diff_pct") <= TOLERANCE_PCT, "match")
                         .otherwise("mismatch")))