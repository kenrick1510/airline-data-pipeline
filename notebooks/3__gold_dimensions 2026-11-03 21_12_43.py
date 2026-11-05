# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

schema = "workspace.cits5504_project"
table_name = f"{schema}.gold_dim_date"

# Parse month-first and year-first dates
parsed_dates = spark.sql(f"""
    SELECT
        departure_date AS original_date,
        CAST(
            COALESCE(
                try_to_timestamp(
                    replace(trim(CAST(departure_date AS STRING)), '-', '/'),
                    'M/d/yyyy'
                ),
                try_to_timestamp(
                    replace(trim(CAST(departure_date AS STRING)), '-', '/'),
                    'yyyy/M/d'
                )
            ) AS DATE
        ) AS departure_date
    FROM {schema}.silver_airline_geography
""")

failed_dates = parsed_dates.filter("departure_date IS NULL")

assert failed_dates.count() == 0, \
    "Missing or invalid departure dates; inspect failed_dates"

date_source = (
    parsed_dates.select("departure_date")
    .distinct()
    .withColumn("year", F.year("departure_date"))
    .withColumn("quarter", F.quarter("departure_date"))
    .withColumn("month", F.month("departure_date"))
)

# Preserve existing date-to-key mappings
existing = spark.table(table_name)

checks = existing.selectExpr(
    "COUNT(*) AS total_rows",
    "COUNT(DISTINCT departure_date) AS unique_dates",
    "COUNT(DISTINCT date_key) AS unique_keys"
).first()

assert checks.total_rows == checks.unique_dates == checks.unique_keys, \
    "Existing dates or date keys are missing/repeated"

max_key = existing.agg(F.max("date_key")).first()[0] or 0

new_dates = (
    date_source
    .join(existing.select("departure_date"), "departure_date", "left_anti")
    .withColumn(
        "date_key",
        F.lit(max_key)
        + F.row_number().over(Window.orderBy("departure_date"))
    )
)

matched_dates = date_source.join(
    existing.select("departure_date", "date_key"),
    "departure_date",
    "inner"
)

columns = ["date_key", "departure_date", "year", "quarter", "month"]

date_updates = matched_dates.select(*columns).unionByName(
    new_dates.select(*columns)
)

date_updates.createOrReplaceTempView("date_updates")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING date_updates AS source
    ON target.departure_date = source.departure_date

    WHEN MATCHED THEN UPDATE SET
        target.year = source.year,
        target.quarter = source.quarter,
        target.month = source.month

    WHEN NOT MATCHED THEN INSERT (
        date_key, departure_date, year, quarter, month
    )
    VALUES (
        source.date_key, source.departure_date,
        source.year, source.quarter, source.month
    )
""")

dim_date = spark.table(table_name)

print("Date rows:", dim_date.count())
display(dim_date.orderBy("departure_date").limit(10))

# COMMAND ----------

table_name = f"{schema}.gold_dim_airport"

airport_source = spark.table(f"{schema}.silver_airports").select(
    F.trim("ident").alias("airport_ident"),
    F.col("name").alias("airport_name"),
    F.col("iata_code"),
    F.col("type").alias("airport_type")
)

# Validate source identifiers
assert airport_source.filter(
    "airport_ident IS NULL OR airport_ident = ''"
).count() == 0, "Missing airport identifier"

assert airport_source.groupBy("airport_ident").count().filter(
    "count > 1"
).count() == 0, "Repeated airport identifier"

# Preserve the existing table and its keys
existing = spark.table(table_name)

checks = existing.selectExpr(
    "COUNT(*) AS total_rows",
    "COUNT(DISTINCT airport_ident) AS unique_ids",
    "COUNT(DISTINCT airport_key) AS unique_keys"
).first()

assert checks.total_rows == checks.unique_ids == checks.unique_keys, \
    "Existing airport identifiers or keys are missing/repeated"

max_key = existing.agg(F.max("airport_key")).first()[0] or 0

# Only new airports receive new surrogate keys
new_airports = (
    airport_source
    .join(existing.select("airport_ident"), "airport_ident", "left_anti")
    .withColumn(
        "airport_key",
        F.lit(max_key)
        + F.row_number().over(Window.orderBy("airport_ident"))
    )
)

matched_airports = airport_source.join(
    existing.select("airport_ident", "airport_key"),
    "airport_ident",
    "inner"
)

columns = [
    "airport_key", "airport_ident", "airport_name",
    "iata_code", "airport_type"
]

airport_updates = matched_airports.select(*columns).unionByName(
    new_airports.select(*columns)
)

airport_updates.createOrReplaceTempView("airport_updates")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING airport_updates AS source
    ON target.airport_ident = source.airport_ident

    WHEN MATCHED THEN UPDATE SET
        target.airport_name = source.airport_name,
        target.iata_code = source.iata_code,
        target.airport_type = source.airport_type

    WHEN NOT MATCHED THEN INSERT (
        airport_key, airport_ident, airport_name,
        iata_code, airport_type
    )
    VALUES (
        source.airport_key, source.airport_ident,
        source.airport_name, source.iata_code,
        source.airport_type
    )
""")

print("Airports saved:", spark.table(table_name).count())
display(spark.table(table_name).orderBy("airport_key").limit(10))

# COMMAND ----------

table_name = f"{schema}.gold_dim_flight_status"

status_source = spark.sql(f"""
    SELECT DISTINCT
        initcap(
            coalesce(nullif(lower(trim(flight_status)), ''), 'unknown')
        ) AS flight_status
    FROM {schema}.silver_airline_geography
""")

existing = spark.table(table_name)

checks = existing.selectExpr(
    "COUNT(*) AS total_rows",
    "COUNT(DISTINCT flight_status) AS unique_statuses",
    "COUNT(DISTINCT flight_status_key) AS unique_keys"
).first()

assert checks.total_rows == checks.unique_statuses == checks.unique_keys, \
    "Existing status values or keys are missing/repeated"

max_key = existing.agg(F.max("flight_status_key")).first()[0] or 0

# Assign keys only to statuses not already stored
new_statuses = (
    status_source
    .join(existing.select("flight_status"), "flight_status", "left_anti")
    .withColumn(
        "flight_status_key",
        F.lit(max_key)
        + F.row_number().over(Window.orderBy("flight_status"))
    )
)

new_statuses.createOrReplaceTempView("new_statuses")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING new_statuses AS source
    ON target.flight_status = source.flight_status

    WHEN NOT MATCHED THEN INSERT (
        flight_status_key, flight_status
    )
    VALUES (
        source.flight_status_key, source.flight_status
    )
""")

print("Flight status rows:", spark.table(table_name).count())
display(spark.table(table_name).orderBy("flight_status_key"))

# COMMAND ----------

airline = spark.table(f"{schema}.silver_airline_geography")

passengers = airline.select(
    "passenger_id", "first_name", "last_name", "gender", "age", "nationality"
).distinct()

conflicts = passengers.groupBy("passenger_id").count().filter("count > 1")
display(conflicts)

assert conflicts.count() == 0, "Review IDs with conflicting passenger details"
passengers.createOrReplaceTempView("passenger_source")

assert passengers.filter(
    "passenger_id IS NULL OR trim(passenger_id) = ''"
).count() == 0, "Missing passenger IDs"

table_name = f"{schema}.gold_dim_passenger"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        passenger_key BIGINT GENERATED ALWAYS AS IDENTITY,
        passenger_id STRING,
        first_name STRING,
        last_name STRING,
        gender STRING,
        age INT,
        nationality STRING
    ) USING DELTA
""")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING passenger_source AS source
    ON target.passenger_id = source.passenger_id
    WHEN MATCHED THEN UPDATE SET
        first_name = source.first_name,
        last_name = source.last_name,
        gender = source.gender,
        age = CAST(source.age AS INT),
        nationality = source.nationality
    WHEN NOT MATCHED THEN INSERT
        (passenger_id, first_name, last_name, gender, age, nationality)
    VALUES
        (source.passenger_id, source.first_name, source.last_name,
         source.gender, CAST(source.age AS INT), source.nationality)
""")

display(spark.table(table_name).orderBy("passenger_key").limit(10))

# COMMAND ----------

table_name = f"{schema}.gold_dim_country"

country_source = spark.sql(f"""
    SELECT
        upper(trim(code)) AS country_code,
        trim(name) AS country_name,
        upper(trim(continent)) AS continent_code
    FROM {schema}.bronze_countries
""")

# Validate source country codes
assert country_source.filter(
    "country_code IS NULL OR country_code = ''"
).count() == 0, "Missing country code"

assert country_source.groupBy("country_code").count().filter(
    "count > 1"
).count() == 0, "Repeated country code"

country_source.createOrReplaceTempView("country_source")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        country_key BIGINT GENERATED ALWAYS AS IDENTITY,
        country_code STRING,
        country_name STRING,
        continent_code STRING
    ) USING DELTA
""")

# Validate existing identifiers and keys
checks = spark.table(table_name).selectExpr(
    "COUNT(*) AS total_rows",
    "COUNT(DISTINCT country_code) AS unique_codes",
    "COUNT(DISTINCT country_key) AS unique_keys"
).first()

assert checks.total_rows == checks.unique_codes == checks.unique_keys, \
    "Existing country codes or keys are missing/repeated"

# Update descriptions while preserving existing country keys
spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING country_source AS source
    ON target.country_code = source.country_code

    WHEN MATCHED THEN UPDATE SET
        target.country_name = source.country_name,
        target.continent_code = source.continent_code

    WHEN NOT MATCHED THEN INSERT (
        country_code, country_name, continent_code
    )
    VALUES (
        source.country_code,
        source.country_name,
        source.continent_code
    )
""")

dim_country = spark.table(table_name)

print("Country rows:", dim_country.count())
display(dim_country.orderBy("country_key").limit(10))

# COMMAND ----------

table_name = f"{schema}.gold_dim_region"

region_source = spark.sql(f"""
    SELECT
        upper(trim(code)) AS region_code,
        trim(name) AS region_name
    FROM {schema}.bronze_regions
""")

assert region_source.filter(
    "region_code IS NULL OR region_code = ''"
).count() == 0, "Missing region code"

assert region_source.groupBy("region_code").count().filter(
    "count > 1"
).count() == 0, "Repeated region code"

region_source.createOrReplaceTempView("region_source")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        region_key BIGINT GENERATED ALWAYS AS IDENTITY,
        region_code STRING,
        region_name STRING
    ) USING DELTA
""")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING region_source AS source
    ON target.region_code = source.region_code

    WHEN MATCHED THEN UPDATE SET
        region_name = source.region_name

    WHEN NOT MATCHED THEN INSERT (region_code, region_name)
    VALUES (source.region_code, source.region_name)
""")

dim_region = spark.table(table_name)

print("Region rows:", dim_region.count())
display(dim_region.orderBy("region_key").limit(10))

# COMMAND ----------

table_name = f"{schema}.gold_dim_pilot"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        pilot_key BIGINT GENERATED ALWAYS AS IDENTITY,
        pilot_name STRING
    ) USING DELTA
""")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING (
        SELECT DISTINCT trim(pilot_name) AS pilot_name
        FROM {schema}.silver_airline_geography
        WHERE pilot_name IS NOT NULL AND trim(pilot_name) <> ''
    ) AS source
    ON target.pilot_name = source.pilot_name
    WHEN NOT MATCHED THEN INSERT (pilot_name)
    VALUES (source.pilot_name)
""")

dim_pilot = spark.table(table_name)
print("Distinct pilot names:", dim_pilot.count())
display(dim_pilot.orderBy("pilot_key").limit(10))

# COMMAND ----------

from pyspark.sql import Window, functions as F

table_name = f"{schema}.gold_fact_passenger_record"

fact = spark.sql(f"""
    SELECT
        d.date_key,
        p.passenger_key,
        a.airport_key,
        c.country_key AS departure_country_key,
        r.region_key AS departure_region_key,
        s.flight_status_key,
        pilot.pilot_key,
        1 AS passenger_record_count
    FROM {schema}.silver_airline_geography AS src

    LEFT JOIN {schema}.gold_dim_date AS d
        ON CAST(
            COALESCE(
                try_to_timestamp(
                    replace(trim(src.departure_date), '-', '/'),
                    'M/d/yyyy'
                ),
                try_to_timestamp(
                    replace(trim(src.departure_date), '-', '/'),
                    'yyyy/M/d'
                )
            ) AS DATE
        ) = d.departure_date

    LEFT JOIN {schema}.gold_dim_passenger AS p
        ON src.passenger_id = p.passenger_id

    LEFT JOIN {schema}.gold_dim_airport AS a
        ON trim(src.airport_ident) = a.airport_ident

    LEFT JOIN {schema}.gold_dim_country AS c
        ON upper(trim(src.airport_country_code)) = c.country_code

    LEFT JOIN {schema}.gold_dim_region AS r
        ON upper(trim(src.airport_region)) = r.region_code

    LEFT JOIN {schema}.gold_dim_flight_status AS s
        ON initcap(
            coalesce(nullif(lower(trim(src.flight_status)), ''), 'unknown')
        ) = s.flight_status

    LEFT JOIN {schema}.gold_dim_pilot AS pilot
        ON trim(src.pilot_name) = pilot.pilot_name
""")

# Validate the incoming records
source_rows = spark.table(
    f"{schema}.silver_airline_geography"
).count()

checks = fact.selectExpr(
    "COUNT(*) AS fact_rows",
    "COUNT(DISTINCT passenger_key) AS unique_passenger_keys",
    "COUNT(*) - COUNT(date_key) AS missing_date_keys",
    "COUNT(*) - COUNT(airport_key) AS missing_airport_keys"
).first()

print("Source rows:", source_rows)
print("Fact rows:", checks.fact_rows)
print("Missing date keys:", checks.missing_date_keys)
print("Missing airport keys:", checks.missing_airport_keys)

assert checks.fact_rows == source_rows, \
    "A dimension join changed the row count."

assert checks.missing_date_keys == 0, \
    "Some departure dates did not match."

assert checks.unique_passenger_keys == checks.fact_rows, \
    "Passenger keys are missing or repeated."

# This cell updates the existing fact table
assert spark.catalog.tableExists(table_name), \
    "Expected the existing fact table to be present."

existing = spark.table(table_name)

existing_checks = existing.selectExpr(
    "COUNT(*) AS total_rows",
    "COUNT(DISTINCT fact_key) AS unique_fact_keys",
    "COUNT(DISTINCT passenger_key) AS unique_passenger_keys"
).first()

assert existing_checks.total_rows == existing_checks.unique_fact_keys, \
    "Existing fact keys are missing or repeated."

assert existing_checks.total_rows == existing_checks.unique_passenger_keys, \
    "Existing passenger keys are missing or repeated."

max_key = existing.agg(F.max("fact_key")).first()[0] or 0

# These candidate keys are used only for new records
fact_updates = fact.withColumn(
    "fact_key",
    F.lit(max_key)
    + F.row_number().over(Window.orderBy("passenger_key"))
)

fact_updates.createOrReplaceTempView("fact_updates")

# Preserve fact_key for existing passengers
spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING fact_updates AS source
    ON target.passenger_key = source.passenger_key

    WHEN MATCHED THEN UPDATE SET
        target.date_key = source.date_key,
        target.airport_key = source.airport_key,
        target.departure_country_key = source.departure_country_key,
        target.departure_region_key = source.departure_region_key,
        target.flight_status_key = source.flight_status_key,
        target.pilot_key = source.pilot_key,
        target.passenger_record_count = source.passenger_record_count

    WHEN NOT MATCHED THEN INSERT (
        fact_key,
        date_key,
        passenger_key,
        airport_key,
        departure_country_key,
        departure_region_key,
        flight_status_key,
        pilot_key,
        passenger_record_count
    )
    VALUES (
        source.fact_key,
        source.date_key,
        source.passenger_key,
        source.airport_key,
        source.departure_country_key,
        source.departure_region_key,
        source.flight_status_key,
        source.pilot_key,
        source.passenger_record_count
    )
""")

fact = spark.table(table_name)

print("Saved fact rows:", fact.count())
display(fact.orderBy("fact_key").limit(10))