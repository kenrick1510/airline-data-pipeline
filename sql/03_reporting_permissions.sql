BEGIN;

CREATE ROLE reporting_reader NOLOGIN;

GRANT USAGE ON SCHEMA cits5504_project TO reporting_reader;

GRANT SELECT ON TABLE
    cits5504_project.fact_passenger_record,
    cits5504_project.dim_date,
    cits5504_project.dim_passenger,
    cits5504_project.dim_airport,
    cits5504_project.dim_country,
    cits5504_project.dim_region,
    cits5504_project.dim_flight_status,
    cits5504_project.dim_pilot
TO reporting_reader;

COMMIT;


SELECT
    has_table_privilege('reporting_reader', 'cits5504_project.fact_passenger_record', 'SELECT') AS can_read,
    has_table_privilege('reporting_reader', 'cits5504_project.fact_passenger_record', 'INSERT') AS can_insert,
    has_table_privilege('reporting_reader', 'cits5504_project.fact_passenger_record', 'UPDATE') AS can_update,
    has_table_privilege('reporting_reader', 'cits5504_project.fact_passenger_record', 'DELETE') AS can_delete;

GRANT reporting_reader TO pbi_report_login;

SELECT pg_has_role('pbi_report_login', 'reporting_reader', 'USAGE')
       AS inherits_reporting_access;


