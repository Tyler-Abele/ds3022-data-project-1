import duckdb
import matplotlib.pyplot as plt
import logging

DB_FILE = "emissions.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log",
)
logger = logging.getLogger(__name__)

PICKUP = {"yellow": "tpep_pickup_datetime", "green": "lpep_pickup_datetime"}
DROPOFF = {"yellow": "tpep_dropoff_datetime", "green": "lpep_dropoff_datetime"}


def one_heavy_light(con, cab, bucket_col, label, pickup_col):
    df = con.execute(
        f"""
        SELECT {bucket_col} AS bucket, AVG(trip_co2_kgs) AS avg_co2 -- find average
        FROM {cab}
        WHERE {pickup_col} IS NOT NULL
          AND EXTRACT(YEAR FROM {pickup_col}) BETWEEN 2015 AND 2024
          AND {bucket_col} IS NOT NULL
        GROUP BY 1
        ORDER BY avg_co2 DESC
    """
    ).fetchdf()
    heavy = df.iloc[0].to_dict() if len(df) else None
    light = df.iloc[-1].to_dict() if len(df) else None
    print(f"{label} (heavy/light): {heavy} | {light}")
    logger.info(f"{cab} - {label}: heavy={heavy}, light={light}")


def run_analysis():
    with duckdb.connect(DB_FILE, read_only=True) as con:
        for cab in ["yellow", "green"]:
            print(f"\n=== {cab.upper()} ANALYSIS (2015-2024) ===")
            pickup_col = PICKUP[cab]
            dropoff_col = DROPOFF[cab]

            # 1) Largest carbon-producing trip (restrict years + non-null)
            res = con.execute(
                f"""
                SELECT trip_distance, trip_co2_kgs,
                       {pickup_col}  AS pickup_datetime,
                       {dropoff_col} AS dropoff_datetime
                FROM {cab}
                WHERE {pickup_col} IS NOT NULL
                  AND EXTRACT(YEAR FROM {pickup_col}) BETWEEN 2015 AND 2024
                ORDER BY trip_co2_kgs DESC
                LIMIT 1
            """
            ).fetchdf()
            print(f"{cab} - Largest carbon-producing trip:\n", res)
            logger.info(f"{cab} - Largest trip: {res.to_dict(orient='records')}")

            # 2–5) Heaviest/lightest by hour/day/week/month
            one_heavy_light(con, cab, "hour_of_day", "Hour of day", pickup_col)
            one_heavy_light(con, cab, "day_of_week", "Day of week", pickup_col)
            one_heavy_light(con, cab, "week_of_year", "Week of year", pickup_col)
            one_heavy_light(con, cab, "month_of_year", "Month", pickup_col)

            res = con.execute(
                f"""
            SELECT 
                strftime({pickup_col}, '%Y-%m') AS year_month,
                SUM(trip_co2_kgs) AS total_co2
            FROM {cab}
            GROUP BY year_month
            ORDER BY year_month
            """
            ).fetchdf()

            plt.plot(res["year_month"], res["total_co2"], label=cab)

        plt.xlabel("Month (1–12)")
        plt.ylabel("Total CO₂ (kg)")
        plt.title("Monthly CO₂ totals (2015–2024)")
        plt.legend()
        plt.tight_layout()
        plt.savefig("monthly_co2.png")
        plt.close()
        logger.info("Saved plot: monthly_co2.png")


if __name__ == "__main__":
    run_analysis()
