{{ config(materialized='table') }}

with cte as(
    SELECT
        f.amount,
        f.booking_date,
        d.airport_id,
        d.airport_name,
        d.city,
        d.country
    FROM
        flights_catalog.gold.fact_bookings f
    LEFT JOIN
        flights_catalog.gold.dim_airports d
    ON f.DimAirportsKey = d.DimAirportsKey
)

SELECT airport_id, airport_name , round(sum(amount),2) as total_amount
FROM cte
GROUP BY 1,2
ORDER BY total_amount DESC