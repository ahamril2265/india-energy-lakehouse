from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NTZ = {"delta.feature.timestampNtz": "supported"}
RELIABLE_DEMAND_PCT = 50


def date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


@dp.materialized_view(name="energy.gold.dim_date",
                      comment="Calendar with Indian seasons and fiscal years (Apr–Mar)")
def dim_date():
    d = spark.sql("SELECT explode(sequence(DATE'2019-01-01', DATE'2025-06-30', INTERVAL 1 DAY)) AS date")
    m = F.month("date")
    season = (F.when(m.isin(12, 1, 2), "Winter")
               .when(m.isin(3, 4, 5), "Summer")
               .when(m.isin(6, 7, 8, 9), "Monsoon")
               .otherwise("Post-monsoon"))
    fy_start = F.when(m >= 4, F.year("date")).otherwise(F.year("date") - 1)
    return d.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        m.alias("month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.trunc("date", "month").alias("month_start"),
        F.date_format("date", "EEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        season.alias("season"),
        F.concat(F.lit("FY"), fy_start.cast("string"), F.lit("-"),
                 F.substring((fy_start + 1).cast("string"), 3, 2)).alias("fiscal_year"),
    )


@dp.materialized_view(name="energy.gold.dim_state",
                      comment="States, UTs and other entities with region and source coverage")
def dim_state():
    official = (spark.read.table("energy.silver.state_monthly_official")
                .groupBy("state")
                .agg(F.first("official_name").alias("official_name"),
                     F.first("name_match").alias("name_match")))
    kaggle = (spark.read.table("energy.silver.state_daily")
              .groupBy("state")
              .agg(F.first("region").alias("kaggle_region"),
                   F.first("state_code").alias("kaggle_code"),
                   F.first("latitude").alias("latitude"),
                   F.first("longitude").alias("longitude")))
    s = official.join(kaggle, "state", "full_outer")
    region = F.coalesce(
        F.col("kaggle_region"),
        F.when(F.col("state") == "DVC", "Eastern")
         .when(F.col("state").isin("Andaman- Nicobar", "Lakshadweep"), "Islands"))
    return s.select(
        "state", "official_name", "name_match",
        region.alias("region"),
        F.when(F.col("state") == "DVC", "utility").otherwise("state_or_ut").alias("entity_type"),
        "latitude", "longitude",
        F.col("official_name").isNotNull().alias("in_official_data"),
        F.col("kaggle_code").isNotNull().alias("in_kaggle_data"),
    )


@dp.materialized_view(name="energy.gold.fact_state_month_energy",
                      comment="Official monthly energy requirement and supply per state (MU)")
def fact_state_month_energy():
    o = spark.read.table("energy.silver.state_monthly_official")
    days = F.dayofmonth(F.last_day("month"))
    return o.select(
        date_key("month").alias("date_key"),
        "state",
        F.col("month").alias("month_start"),
        "requirement_mu",
        F.col("availability_mu").alias("energy_met_mu"),
        (F.col("requirement_mu") - F.col("availability_mu")).alias("shortfall_mu"),
        F.round((F.col("requirement_mu") - F.col("availability_mu"))
                / F.col("requirement_mu") * 100, 3).alias("shortfall_pct"),
        days.alias("days_in_month"),
        F.round(F.col("availability_mu") / days, 2).alias("avg_daily_mu"),
        F.col("implausible").alias("is_implausible"),
    )


@dp.materialized_view(name="energy.gold.fact_grid_hourly",
                      comment="National grid per hour: demand and generation by source, renewable share",
                      table_properties=NTZ)
def fact_grid_hourly():
    g = spark.read.table("energy.silver.grid_hourly")
    return g.select(
        date_key("hour").alias("date_key"),
        "hour",
        F.hour("hour").alias("hour_of_day"),
        "demand_mw", "thermal_mw", "hydro_mw", "gas_mw", "nuclear_mw",
        "wind_mw", "solar_mw", "total_generation_mw",
        F.round(F.col("demand_mw") / 1000, 4).alias("demand_mu"),   # avg MW over 1 h = MWh; /1000 = MU
        F.round((F.col("solar_mw") + F.col("wind_mw")) / F.col("demand_mw") * 100, 2)
         .alias("solar_wind_share_pct"),
        "completeness_pct", "demand_completeness_pct",
        (F.col("demand_completeness_pct") >= RELIABLE_DEMAND_PCT).alias("demand_reliable"),
        "source_format", "resolution_seconds",
    )


@dp.materialized_view(name="energy.gold.fact_state_daily_validated",
                      comment="Kaggle daily state data, only for state-months that matched official figures")
def fact_state_daily_validated():
    matched = (spark.read.table("energy.silver.state_month_reconciliation")
               .where("status = 'match'").select("state", "month"))
    daily = spark.read.table("energy.silver.state_daily").withColumn("month", F.trunc("date", "month"))
    return (daily.join(matched, ["state", "month"])
            .select(date_key("date").alias("date_key"), "state", "date",
                    "usage_mu", "suspicious_jump"))
    
@dp.materialized_view(name="energy.gold.crosscheck_national_monthly",
                      comment="Sum of official state energy vs national grid demand, per month (MU)")
def crosscheck_national_monthly():
    states = (spark.read.table("energy.gold.fact_state_month_energy")
              .groupBy("month_start")
              .agg(F.count("*").alias("entities_reported"),
                   F.sum(F.col("is_implausible").cast("int")).alias("implausible_entities"),
                   F.sum("energy_met_mu").alias("states_total_mu")))
    states = (states
              .withColumn("max_entities", F.max("entities_reported").over(Window.partitionBy()))
              .withColumn("states_complete",
                          (F.col("entities_reported") == F.col("max_entities"))
                          & (F.col("implausible_entities") == 0)))

    grid = (spark.read.table("energy.gold.fact_grid_hourly")
            .where("demand_reliable")
            .withColumn("month_start", F.trunc(F.to_date("hour"), "month"))
            .groupBy("month_start")
            .agg(F.count("*").alias("reliable_hours"),
                 F.avg("demand_mw").alias("avg_demand_mw"))
            .withColumn("hours_in_month", F.dayofmonth(F.last_day("month_start")) * 24)
            .withColumn("grid_coverage_pct",
                        F.round(F.col("reliable_hours") / F.col("hours_in_month") * 100, 1))
            .withColumn("grid_total_mu",
                        F.round(F.col("avg_demand_mw") * F.col("hours_in_month") / 1000, 2)))

    diff_pct = (F.col("grid_total_mu") - F.col("states_total_mu")) / F.col("states_total_mu") * 100

    return (states.join(grid, "month_start")          # overlap months only
            .select("month_start", "entities_reported", "states_complete",
                    F.round("states_total_mu", 2).alias("states_total_mu"),
                    "grid_total_mu", "grid_coverage_pct",
                    F.round(diff_pct, 2).alias("diff_pct")))