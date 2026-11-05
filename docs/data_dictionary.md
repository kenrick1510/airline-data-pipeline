# Data Dictionary

Databricks tables use schema `workspace.cits5504_project`.
Lakebase reporting tables use schema `cits5504_project` and omit
the `gold_` prefix.

Types below reflect the notebook definitions and transformations.
Numeric key types in pre-existing tables should be confirmed using
DESCRIBE TABLE.

## gold_fact_passenger_record

One row per passenger record in the supplied dataset.

| Column | Description |
| --- | --- |
| fact_key | Surrogate identifier for the fact record |
| date_key | Reference to gold_dim_date |
| passenger_key | Reference to gold_dim_passenger and current MERGE identifier |
| airport_key | Reference to gold_dim_airport, nullable for unmatched airports |
| departure_country_key | Reference to gold_dim_country |
| departure_region_key | Reference to gold_dim_region |
| flight_status_key | Reference to gold_dim_flight_status |
| pilot_key | Reference to gold_dim_pilot |
| passenger_record_count | Integer measure set to 1 for each source record |

The measure counts passenger records. A separate flight identifier would
be required to calculate distinct flight counts.

## gold_dim_date

One row per observed departure date.

| Column | Type / description |
| --- | --- |
| date_key | Numeric surrogate key |
| departure_date | DATE, natural identifier |
| year | INT, calendar year |
| quarter | INT, quarter from 1 to 4 |
| month | INT, month from 1 to 12 |

## gold_dim_passenger

One row per passenger identifier.

| Column | Type / description |
| --- | --- |
| passenger_key | BIGINT identity surrogate key |
| passenger_id | STRING, source identifier |
| first_name | STRING, passenger first name |
| last_name | STRING, passenger last name |
| gender | STRING, source gender value |
| age | INT, passenger age |
| nationality | STRING, source nationality |

The load checks for conflicting descriptions associated with the same
passenger identifier.

## gold_dim_airport

One row per airport identifier.

| Column | Type / description |
| --- | --- |
| airport_key | Numeric surrogate key |
| airport_ident | STRING, natural identifier from the airport reference |
| airport_name | STRING, airport name |
| iata_code | STRING, cleaned IATA code, nullable |
| airport_type | STRING, reference airport classification |

## gold_dim_country

One row per country code.

| Column | Type / description |
| --- | --- |
| country_key | BIGINT identity surrogate key |
| country_code | STRING, uppercased country code |
| country_name | STRING, country name |
| continent_code | STRING, continent code |

## gold_dim_region

One row per region code.

| Column | Type / description |
| --- | --- |
| region_key | BIGINT identity surrogate key |
| region_code | STRING, uppercased region code |
| region_name | STRING, region name |

Country and region connect directly to the fact table.

## gold_dim_flight_status

One row per cleaned flight-status value.

| Column | Type / description |
| --- | --- |
| flight_status_key | Numeric surrogate key |
| flight_status | STRING, cleaned and title-cased status |

Null or blank status values are mapped to Unknown.

## gold_dim_pilot

One row per distinct trimmed pilot name.

| Column | Type / description |
| --- | --- |
| pilot_key | BIGINT identity surrogate key |
| pilot_name | STRING, natural identifier used by this dataset |

Null and blank names are excluded. Different pilots sharing the same
name would require a stronger source identifier.

## Lineage and matching fields

| Column | Description |
| --- | --- |
| _source_file | Source CSV path |
| _ingested_at | Bronze ingestion timestamp |
| _batch_id | Load batch label |
| departure_airport_iata_code | Cleaned airport code used for matching |
| airport_match_method | Records IATA, name/country or unresolved matching outcomes |
