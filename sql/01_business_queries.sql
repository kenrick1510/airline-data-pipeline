CREATE SCHEMA IF NOT EXISTS airline_dw;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT country_key) AS unique_country_keys
FROM cits5504_project.dim_country;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT airport_key) AS unique_airport_keys
FROM cits5504_project.dim_airport;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT date_key) AS unique_airport_keys
FROM cits5504_project.dim_date;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT flight_status_key) AS unique_airport_keys
FROM cits5504_project.dim_flight_status;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT passenger_key) AS unique_airport_keys
FROM cits5504_project.dim_passenger;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT pilot_key) AS unique_airport_keys
FROM cits5504_project.dim_pilot;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region_key) AS unique_airport_keys
FROM cits5504_project.dim_region;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT fact_key) AS unique_airport_keys
FROM cits5504_project.fact_passenger_record;

/* : How many passenger records were delayed each month by country and airport */
SELECT
    DATE_TRUNC('month', d.departure_date)::date AS month,
    c.country_code,
    a.airport_ident,
    SUM(f.passenger_record_count) AS delayed_records
FROM cits5504_project.fact_passenger_record f
JOIN cits5504_project.dim_date d ON f.date_key = d.date_key
JOIN cits5504_project.dim_country c ON f.departure_country_key = c.country_key
JOIN cits5504_project.dim_airport a ON f.airport_key = a.airport_key
JOIN cits5504_project.dim_flight_status s ON f.flight_status_key = s.flight_status_key
WHERE LOWER(s.flight_status) = 'delayed'
GROUP BY DATE_TRUNC('month', d.departure_date)::date, c.country_code, a.airport_ident
ORDER BY month, delayed_records DESC;

/* Which regions recorded the most cancellations among passengers aged 65 and over in each country */
SELECT
    c.country_code,
    r.region_code,
    SUM(f.passenger_record_count) AS cancelled_records
FROM cits5504_project.fact_passenger_record f
JOIN cits5504_project.dim_country c ON f.departure_country_key = c.country_key
JOIN cits5504_project.dim_region r ON f.departure_region_key = r.region_key
JOIN cits5504_project.dim_passenger p ON f.passenger_key = p.passenger_key
JOIN cits5504_project.dim_flight_status s ON f.flight_status_key = s.flight_status_key
WHERE p.age::integer >= 65
  AND LOWER(s.flight_status) IN ('cancelled', 'canceled')
GROUP BY c.country_code, r.region_code
ORDER BY c.country_code, cancelled_records DESC;

 /* Which airports have the highest cancellation rates within each country? */
/* Q3: What proportion of passenger records were on time,
   delayed or cancelled for each country and airport? */

WITH status_counts AS (
    SELECT
        c.country_code,
        a.airport_ident,
        s.flight_status,
        SUM(f.passenger_record_count) AS passenger_records
    FROM cits5504_project.fact_passenger_record AS f
    JOIN cits5504_project.dim_country AS c
        ON f.departure_country_key = c.country_key
    JOIN cits5504_project.dim_airport AS a
        ON f.airport_key = a.airport_key
    JOIN cits5504_project.dim_flight_status AS s
        ON f.flight_status_key = s.flight_status_key
    GROUP BY
        c.country_code,
        a.airport_ident,
        s.flight_status
)
SELECT
    country_code,
    airport_ident,
    flight_status,
    passenger_records,
    ROUND(
        100.0 * passenger_records
        / NULLIF(
            SUM(passenger_records) OVER (
                PARTITION BY country_code, airport_ident
            ), 0
        ),
        2
    ) AS percentage
FROM status_counts
ORDER BY country_code, airport_ident, flight_status;

/* How did passenger volume vary by quarter, region and age group? */
SELECT
    DATE_TRUNC('quarter', d.departure_date)::date AS quarter_start,
    r.region_code,
    CASE
        WHEN p.age IS NULL THEN 'Unknown'
        WHEN p.age::integer < 18 THEN 'Under 18'
        WHEN p.age::integer < 65 THEN '18–64'
        ELSE '65+'
    END AS age_group,
    SUM(f.passenger_record_count) AS passenger_records
FROM cits5504_project.fact_passenger_record f
JOIN cits5504_project.dim_date d ON f.date_key = d.date_key
JOIN cits5504_project.dim_region r ON f.departure_region_key = r.region_key
JOIN cits5504_project.dim_passenger p ON f.passenger_key = p.passenger_key
GROUP BY quarter_start, r.region_code, age_group
ORDER BY quarter_start, r.region_code, passenger_records DESC;

/* Which pilot–airport pairs recorded the most delays in each country and quarter? */
SELECT
    DATE_TRUNC('quarter', d.departure_date)::date AS quarter_start,
    c.country_code,
    a.airport_ident,
    p.pilot_name,
    SUM(f.passenger_record_count) AS delayed_records
FROM cits5504_project.fact_passenger_record f
JOIN cits5504_project.dim_date d ON f.date_key = d.date_key
JOIN cits5504_project.dim_country c ON f.departure_country_key = c.country_key
JOIN cits5504_project.dim_airport a ON f.airport_key = a.airport_key
JOIN cits5504_project.dim_pilot p ON f.pilot_key = p.pilot_key
JOIN cits5504_project.dim_flight_status s ON f.flight_status_key = s.flight_status_key
WHERE LOWER(s.flight_status) = 'delayed'
GROUP BY quarter_start, c.country_code, a.airport_ident, p.pilot_key, p.pilot_name
ORDER BY quarter_start, c.country_code, delayed_records DESC;