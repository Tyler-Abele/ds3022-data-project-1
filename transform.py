import duckdb
import logging

# This transform script works great but I also did DBT that also works.

DB_FILE = "emissions.duckdb"
LOG_FILE = "transform.log"


# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)


# Build features table with new columns
def build_features(con, src, dst, pickup_col, dropoff_col, vehicle_type):

    sql = f"""
    CREATE OR REPLACE TABLE {dst} AS
    WITH params AS (
      SELECT co2_grams_per_mile AS co2_gpm
      FROM vehicle_emissions
      WHERE vehicle_type = '{vehicle_type}'
      LIMIT 1
    ),
    base AS (
      SELECT
        t.*,
        DATEDIFF('second', {pickup_col}, {dropoff_col}) AS trip_seconds
      FROM {src} t
    )
    SELECT
      b.*,
      ROUND(b.trip_distance * p.co2_gpm / 1000.0, 6) AS trip_co2_kgs,
      CASE WHEN b.trip_seconds > 0
           THEN b.trip_distance / (b.trip_seconds / 3600.0)
           ELSE NULL
      END AS avg_mph,
      EXTRACT(HOUR FROM {pickup_col})           AS hour_of_day,
      EXTRACT(DOW  FROM {pickup_col})           AS day_of_week,   
      EXTRACT(WEEK FROM {pickup_col})           AS week_of_year,
      EXTRACT(MONTH FROM {pickup_col})          AS month_of_year
    FROM base b
    CROSS JOIN params p;
    """

    con.execute(sql)
    n = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]  # count rows
    logger.info(f"Built {dst} with {n:,} rows")  # log it
    print(f"Built {dst}: {n:,} rows")


# main transform function -- calls build_features twice once
def main():
    try:
        with duckdb.connect(DB_FILE, read_only=False) as con:
            logger.info("Connected to DuckDB")

            # Feature table for Yellow
            build_features(
                con,
                src="yellow",
                dst="yellow",
                pickup_col="tpep_pickup_datetime",
                dropoff_col="tpep_dropoff_datetime",
                vehicle_type="yellow_taxi",
            )

            # Feature table for Green
            build_features(
                con,
                src="green",
                dst="green",
                pickup_col="lpep_pickup_datetime",
                dropoff_col="lpep_dropoff_datetime",
                vehicle_type="green_taxi",
            )

            logger.info("Transform complete")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception("Transform failed")


if __name__ == "__main__":
    main()
