# Run Instructions

## Requirements

- A Databricks workspace with compatible PySpark and Delta Lake support.
- Access to `workspace.cits5504_project`.
- A volume at `/Volumes/workspace/cits5504_project/raw_data/`.
- Lakebase PostgreSQL for the reporting tables.
- Power BI Desktop for the report.

Place these source files in the volume:

- Airline Dataset Updated - v2.csv
- airports.csv
- countries.csv
- regions.csv
- navaids.csv

Dataset download links and licensing details must be supplied separately.

## Initial setup

Import the Python source files from `notebooks/` into Databricks.

The exported Gold notebook expects the following tables to exist:

- gold_dim_date
- gold_dim_airport
- gold_dim_flight_status
- gold_fact_passenger_record

Their initial creation scripts are required for a fresh deployment.
The remaining dimension definitions are included in the Gold notebook.

## Execution order

1. Run the Bronze ingestion notebook.
2. After Bronze succeeds, run the Silver notebook.
3. After Silver succeeds, run the Gold notebook.
4. Run the validation notebook using the procedure below.

Run each notebook from the beginning so its variables and temporary
views are created in the correct order.

## Repeatability test

1. Complete and review an initial Gold load.
2. Run the baseline-creation cell in the validation notebook.
3. Rerun Silver and Gold with the same source snapshot.
4. Run the validation notebook again.
5. Confirm all eight tables show PASS, with zero added/changed and
   zero removed/changed rows.

Existing audit tables preserve the original baseline. Keep these tables
during the test.

Creating a baseline and immediately comparing against it checks the
current snapshot. Demonstrating repeatability requires a load between
baseline creation and the final comparison.

## Scheduled execution

Configure notebook tasks with these dependencies:

Bronze → Silver → Gold

The project schedule is 08:00 Australia/Perth.

Use a single concurrent run because some Gold keys are allocated from
the current maximum key plus row numbers. Avoid overlapping manual
loads.

Configure task retries and failure notifications in Databricks.
Record their settings and test results as operational evidence.

## Lakebase reporting

Configure synchronisation or export mappings from Gold into Lakebase:

- `gold_dim_*` becomes `dim_*`.
- `gold_fact_passenger_record` becomes `fact_passenger_record`.

Preserve surrogate keys and verify synchronisation before refreshing
Power BI. These mappings are configured outside the exported notebooks.

Run the business and performance SQL in Lakebase PostgreSQL.

## Reporting permissions

Create `pbi_report_login` as a password login through Lakebase.

Using an authorised administrator, run the reporting-role creation and
grants from `sql/03_reporting_permissions.sql`.

`CREATE ROLE reporting_reader` is a one-time setup command. If the role
already exists, run the grants and checks separately.

Verify that the login inherits the reporting role and that SELECT is
allowed while INSERT, UPDATE and DELETE are denied on the tested table.

## Power BI

Enter the Lakebase connection details and use `pbi_report_login` with
its generated password in the data-source credential settings.

Refresh the report and confirm that all required tables load.

For the flight-status donut chart, use flight status as the legend and
passenger-record counts as values. Country and airport slicers determine
the selected reporting context.

## Performance verification

Run the original and revised Q1 queries separately with
`EXPLAIN (ANALYZE, BUFFERS)`.

Use the same database, compute and data snapshot. Keep the final sorting
in both queries and record several executions.

Run the EXCEPT ALL comparison separately. Both difference counts should
equal zero.
