from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/energy/bronze/landing"

GRID_COLS = ["ts_raw", "thermal_mw", "hydro_mw", "gas_mw", "nuclear_mw", "wind_mw",
             "solar_mw", "demand_mw", "total_generation_mw",
             "source_file", "source_format", "resolution_seconds"]
STATE_COLS = ["States", "Regions", "latitude", "longitude", "Dates", "Usage"]


def autoload_csv(path, cols):
    """Incrementally ingest CSVs as all-string columns, keeping lineage."""
    schema = ", ".join(f"`{c}` STRING" for c in cols)
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(schema)
            .load(path)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_landing_file", F.col("_metadata.file_path")))


@dp.table(
    name="energy.bronze.grid_readings",
    comment="Raw national grid readings (5 s, 10 s and hourly), one row per source reading",
)
def grid_readings():
    return autoload_csv(f"{LANDING}/grid/", GRID_COLS)


@dp.table(
    name="energy.bronze.state_daily_raw",
    comment="Raw daily state-wise consumption (MU) scraped from POSOCO weekly reports",
)
def state_daily_raw():
    return autoload_csv(f"{LANDING}/state_daily/", STATE_COLS)