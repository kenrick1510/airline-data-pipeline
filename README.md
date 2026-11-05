# Airline-Data-Pipeline

A data engineering project using Databricks, PySpark, Delta Lake,
Lakebase PostgreSQL and Power BI to process and analyse airline
passenger records.

## Architecture

CSV source files → Bronze → Silver → Gold → Lakebase → Power BI

- Bronze loads source files and records ingestion metadata.
- Silver cleans airport codes, parses dates and enriches records
  using airport and region reference data.
- Gold builds seven dimensions and one passenger-record fact table.
- Lakebase provides SQL reporting tables for Power BI.

## Technologies

Python, PySpark, SQL, Databricks, Delta Lake, Lakebase PostgreSQL,
Power BI and GitHub.

## Results

- Processed 98,619 passenger records.
- Validated seven dimensions and one fact table against saved
  baselines, with zero differing rows on the tested reruns.
- Preserved existing surrogate keys during repeated Gold loads.
- Configured Bronze → Silver → Gold task dependencies and a daily
  08:00 Australia/Perth schedule.
- Recorded a successful manual pipeline run.
- Reduced median execution time for the monthly-delay query from
  75.85 ms to 68.85 ms across four runs, approximately 9.23%.
- Confirmed identical original and revised query results.
- Refreshed Power BI using a dedicated reporting login with
  inherited SELECT permissions.

## Repository structure

| Folder | Contents |
| --- | --- |
| notebooks | Bronze, Silver, Gold and validation notebooks |
| sql | Business queries, performance comparison and reporting permissions |
| docs | Project documentation and validation evidence |

## Business questions

1. How many passenger records were delayed each month by country
   and airport?
2. Which regions recorded the most cancellations among passengers
   aged 65 and over in each country?
3. What proportion of passenger records were on time, delayed or
   cancelled for each country and airport?
4. How did passenger volume vary by quarter, region and age group?
5. Which pilot–airport pairs recorded the most delays in each
   country and quarter?

## Current limitations

- Bronze ingestion skips existing tables.
- Repeatability was tested using the same source snapshot.
- Gold currently expects several tables to exist before loading.
  Initial setup and Lakebase synchronisation require additional
  configuration.
- The recorded fact load retained 1,476 unmatched airport keys.
  Airport-based SQL reports exclude these through inner joins.
- The fact grain assumes one record per passenger identifier in
  this dataset.
- Performance measurements depend on compute, cache and workload.
