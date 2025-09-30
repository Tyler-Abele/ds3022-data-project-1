import duckdb
import logging

# name files
DB_FILE = "emissions.duckdb"
LOG_FILE = "clean.log"


# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)


# Build the in-place cleaning SQL given the time column names
def cleaning_sql(tbl: str, pickup_ts: str, dropoff_ts: str) -> str:
    return f"""
CREATE OR REPLACE TABLE {tbl} AS
WITH base AS (
  SELECT
    *,
    DATEDIFF('second', {pickup_ts}, {dropoff_ts}) AS trip_seconds
  FROM {tbl}
),
filtered AS (
  SELECT *
  FROM base
  WHERE passenger_count IS NOT NULL AND > 0
    AND trip_distance IS NOT NULL AND > 0
    AND trip_distance IS NOT NULL AND <= 100
    AND {pickup_ts} IS NOT NULL
    AND {dropoff_ts} IS NOT NULL
    AND trip_seconds BETWEEN 0 AND 86400
),
-- Dedupe definition: same pickup/dropoff time, same distance, same passenger_count
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY {pickup_ts}, {dropoff_ts}, trip_distance, passenger_count
      ORDER BY {pickup_ts} -- keep earliest in tie
    ) AS rn
  FROM filtered
)
SELECT * EXCLUDE (rn, trip_seconds) FROM deduped
WHERE rn = 1;
"""


# Verification queries (should all be 0)
def verify_queries(tbl: str, pickup_ts: str, dropoff_ts: str) -> dict:
    return {
        "zero_passengers": f"""
            SELECT COUNT(*) FROM {tbl}
            WHERE passenger_count <= 0 OR passenger_count IS NULL;
        """,
        "zero_distance": f"""
            SELECT COUNT(*) FROM {tbl}
            WHERE trip_distance <= 0 OR trip_distance IS NULL;
        """,
        "over_100_miles": f"""
            SELECT COUNT(*) FROM {tbl}
            WHERE trip_distance > 100;
        """,
        "over_1_day": f"""
            SELECT COUNT(*) FROM {tbl}
            WHERE DATEDIFF('second', {pickup_ts}, {dropoff_ts}) > 86400
               OR DATEDIFF('second', {pickup_ts}, {dropoff_ts}) < 0
               OR {pickup_ts} IS NULL OR {dropoff_ts} IS NULL;
        """,
        "duplicates": f"""
            SELECT COUNT(*) FROM (
              SELECT {pickup_ts}, {dropoff_ts}, trip_distance, passenger_count, COUNT(*) c
              FROM {tbl}
              GROUP BY 1,2,3,4
              HAVING COUNT(*) > 1
            );
        """,
    }


def clean_table(con, tbl: str, pickup_ts: str, dropoff_ts: str):
    raw = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # before clean
    con.execute(cleaning_sql(tbl, pickup_ts, dropoff_ts))  # run clean
    clean = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # after clean
    msg = f"{tbl} (raw): {raw:,} | (cleaned): {clean:,} | removed: {raw - clean:,}"
    print(msg)  # let user know
    logger.info(msg)

    print(f"Verification for {tbl}:")  # verify
    logger.info(f"Verification for {tbl}:")
    all_ok = True
    for name, q in verify_queries(tbl, pickup_ts, dropoff_ts).items():
        n = con.execute(q).fetchone()[0]
        line = f"{name:>16}: {n}"
        print(" ", line)
        logger.info(line)
        all_ok &= n == 0
    # make sure all constraints are satisfied
    if all_ok:
        print(" ✅ All constraints satisfied.\n")
        logger.info("All constraints satisfied.")
    else:
        print(" ⚠️ Issues remain.\n")
        logger.warning("One or more constraints still present.")


# main func
def main():
    try:
        with duckdb.connect(DB_FILE, read_only=False) as con:
            logger.info("Connected to DuckDB")

            # In-place clean Yellow (tpep_* timestamps)
            clean_table(
                con,
                "yellow",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
            )

            # In-place clean Green (lpep_* timestamps)
            clean_table(con, "green", "lpep_pickup_datetime", "lpep_dropoff_datetime")

            logger.info("Cleaning complete")
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception("Cleaning failed")


if __name__ == "__main__":
    main()
