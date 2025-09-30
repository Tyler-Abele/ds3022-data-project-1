import duckdb
import logging


BUCKET = "dp-1p3022"
PREFIX = "trip-data"  # s3 folder
DB_FILE = "emissions.duckdb"  # duckdb
EMISSIONS_CSV = "data/vehicle_emissions.csv"
LOG_FILE = "load.log"

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_FILE,
)

logger = logging.getLogger(__name__)


# Generate S3 HTTPS URLs for Parquet files
def s3_https_urls(color: str, year: int) -> list[str]:
    """Generate URLs for all months of a given year"""
    base = f"https://{BUCKET}.s3.amazonaws.com/{PREFIX}"
    return [f"{base}/{color}_tripdata_{year}-{m:02d}.parquet" for m in range(1, 13)]


# Generate S3 HTTPS URLs for multiple years -- utilize loop
def s3_https_urls_multi_year(color: str, start_year: int, end_year: int) -> list[str]:
    """Generate URLs for all months across multiple years"""
    urls = []
    for year in range(start_year, end_year + 1):
        urls.extend(s3_https_urls(color, year))
    return urls


# main load function
def main():
    try:
        with duckdb.connect(DB_FILE, read_only=False) as con:
            logger.info("Connected to DuckDB")

            # Enable HTTP/HTTPS access so that we can use s3
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")

            # ---- Yellow 2015-2024 ----
            y_urls = s3_https_urls_multi_year("yellow", 2015, 2024)
            con.execute(
                """
                CREATE OR REPLACE TABLE yellow AS
                SELECT * FROM read_parquet($urls, union_by_name=TRUE)
                """,
                {"urls": y_urls},
            )
            logger.info("Created/loaded table yellow")

            # ---- Green 2015-2024 ----
            g_urls = s3_https_urls_multi_year("green", 2015, 2024)
            con.execute(
                """
                CREATE OR REPLACE TABLE green AS
                SELECT * FROM read_parquet($urls, union_by_name=TRUE)
                """,
                {"urls": g_urls},
            )
            logger.info("Created/loaded table green")

            # ---- Vehicle emissions lookup ----
            con.execute(
                f"""
                CREATE OR REPLACE TABLE vehicle_emissions AS
                SELECT * FROM read_csv_auto('{EMISSIONS_CSV}', HEADER=TRUE)
                """
            )
            logger.info("Created/loaded table vehicle_emissions")

            # ---- Row counts ----
            for tbl in ("yellow", "green", "vehicle_emissions"):
                cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                print(f"Table {tbl}: {cnt:,} rows (before cleaning)")
                logger.info(f"{tbl} row count: {cnt}")

            logger.info("Load complete")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception("Load failed")


if __name__ == "__main__":
    main()
