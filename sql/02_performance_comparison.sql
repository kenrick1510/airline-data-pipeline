WITH original AS (
    SELECT
        DATE_TRUNC('month', d.departure_date)::date AS month_start,
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
),
date_month AS MATERIALIZED (
    SELECT date_key, DATE_TRUNC('month', departure_date)::date AS month_start
    FROM cits5504_project.dim_date
),
revised AS (
    SELECT
        d.month_start,
        c.country_code,
        a.airport_ident,
        SUM(f.passenger_record_count) AS delayed_records
    FROM cits5504_project.fact_passenger_record f
    JOIN date_month d ON f.date_key = d.date_key
    JOIN cits5504_project.dim_country c ON f.departure_country_key = c.country_key
    JOIN cits5504_project.dim_airport a ON f.airport_key = a.airport_key
    JOIN cits5504_project.dim_flight_status s ON f.flight_status_key = s.flight_status_key
    WHERE LOWER(s.flight_status) = 'delayed'
    GROUP BY d.month_start, c.country_code, a.airport_ident
)
SELECT 'original_only' AS difference, COUNT(*) AS rows
FROM (SELECT * FROM original EXCEPT ALL SELECT * FROM revised) a
UNION ALL
SELECT 'revised_only', COUNT(*)
FROM (SELECT * FROM revised EXCEPT ALL SELECT * FROM original) b;

--| Version | Run | Execution time | Planning time |
--|---|---:|---:|---:|
--| Original | 1 | 79.4 ms | 0.709 ms |
--| Original | 2 | 75.6 ms | 0.394 ms |
--| Original | 3 | 76.1 ms | 0.395 ms |
--| Original | 4 | 75.5 ms | 0.395 ms |
--| Revised | 1 | 69.7 ms | 0.361 ms |
--| Revised | 2 | 68.8 ms | 0.331 ms |
--| Revised | 3 | 68.3 ms | 0.346 ms |
--| Revised | 4 | 68.9 ms | 0.359 ms |