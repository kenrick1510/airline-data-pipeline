# Validation Results

The results below were recorded during project development. They
describe the tested source snapshot and execution environment.

## Gold repeatability

The consolidated validation report showed that all seven dimensions
and the fact table matched their saved baselines.

| Table | Rows | Added/changed | Removed/changed |
| --- | ---: | ---: | ---: |
| gold_dim_date | 364 | 0 | 0 |
| gold_dim_passenger | 98,619 | 0 | 0 |
| gold_dim_airport | 84,536 | 0 | 0 |
| gold_dim_country | 249 | 0 | 0 |
| gold_dim_region | 3,942 | 0 | 0 |
| gold_dim_flight_status | 3 | 0 | 0 |
| gold_dim_pilot | 98,605 | 0 | 0 |
| gold_fact_passenger_record | 98,619 | 0 | 0 |

The checks covered:

- Unique, non-null surrogate keys.
- Unique, non-null record identifiers.
- Blank identifiers.
- Full-row differences in both directions, including duplicates.

All eight tables returned PASS for the recorded comparison.

## Fact quality

| Check | Recorded result |
| --- | ---: |
| Source passenger records | 98,619 |
| Fact records | 98,619 |
| Missing date keys | 0 |
| Missing airport keys | 1,476 |

Unmatched airport records were retained in the fact table.
Airport-based reports using inner joins exclude these records.

## Query performance

Q1 calculates monthly delayed passenger records by country and airport.

The revised query calculates month values in a materialised date CTE
before joining the fact table.

| Version | Run | Execution time ms | Planning time ms |
| --- | ---: | ---: | ---: |
| Original | 1 | 79.4 | 0.709 |
| Original | 2 | 75.6 | 0.394 |
| Original | 3 | 76.1 | 0.395 |
| Original | 4 | 75.5 | 0.395 |
| Revised | 1 | 69.7 | 0.361 |
| Revised | 2 | 68.8 | 0.331 |
| Revised | 3 | 68.3 | 0.346 |
| Revised | 4 | 68.9 | 0.359 |

Median execution time across all four runs:

- Original — 75.85 ms.
- Revised — 68.85 ms.
- Observed reduction — approximately 9.23%.

The result-equivalence check returned:

| Difference | Rows |
| --- | ---: |
| original_only | 0 |
| revised_only | 0 |

These measurements depend on compute, cache state and competing workload.
The original execution plan reported shared buffer hits.

## Reporting access

The recorded fact-table permission check for `reporting_reader` returned:

| Permission | Result |
| --- | --- |
| SELECT | Allowed |
| INSERT | Denied |
| UPDATE | Denied |
| DELETE | Denied |

The `pbi_report_login` account inherited the reporting role.
Power BI refreshed successfully using this dedicated login.

## Pipeline execution

A manual Bronze → Silver → Gold job run succeeded in approximately
3 minutes 7 seconds.

The configured schedule was 08:00 Australia/Perth.
Scheduled-run outcomes, retry behaviour and notification delivery
require separate evidence.

## Evidence to attach

Add screenshots showing:

- The consolidated eight-table validation report.
- The successful job run and task dependencies.
- The original and revised query execution results.
- The zero-row result-equivalence comparison.
- The reporting permission checks.

## Verification limits

The baseline test demonstrates repeatability for the tested snapshot.
Additional tests are required for new records, changed source data,
concurrent execution and complete foreign-key coverage.

The latest Silver code cleanup was reviewed from its export. A fresh
execution result after that cleanup should be retained with the project.
