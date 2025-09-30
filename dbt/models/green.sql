{{ config(materialized='table') }}

WITH params AS (
  SELECT co2_grams_per_mile AS co2_gpm FROM vehicle_emissions WHERE vehicle_type = 'green_taxi'
),
base AS (
  SELECT *,
         DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) AS trip_seconds
  FROM green
)
SELECT
  b.*,
  ROUND(b.trip_distance * p.co2_gpm / 1000.0, 6) AS trip_co2_kgs,
  CASE WHEN b.trip_seconds > 0
       THEN b.trip_distance / (b.trip_seconds / 3600.0)
       ELSE NULL END AS avg_mph,
  EXTRACT(HOUR FROM lpep_pickup_datetime)  AS hour_of_day,
  EXTRACT(DOW  FROM lpep_pickup_datetime)  AS day_of_week,
  EXTRACT(WEEK FROM lpep_pickup_datetime)  AS week_of_year,
  EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year,
  EXTRACT(YEAR FROM lpep_pickup_datetime)   AS year
FROM base b
CROSS JOIN params p
