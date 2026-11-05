# Databricks notebook source
base_path = "/Volumes/workspace/cits5504_project/raw_data/"

display(dbutils.fs.ls(base_path))

# COMMAND ----------

#airport file
airports_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(base_path + "airports.csv")
)

display(airports_df.limit(10))
print("Rows:", airports_df.count())

# COMMAND ----------

Airlines_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(base_path + "Airline Dataset Updated - v2.csv")
)

display(Airlines_df.limit(10))
print("Rows:", Airlines_df.count())

# COMMAND ----------

countries_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(base_path + "countries.csv")
)

display(countries_df.limit(10))
print("Rows:", countries_df.count())

# COMMAND ----------

regions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(base_path + "regions.csv")
)

display(regions_df.limit(10))
print("Rows:", regions_df.count())

# COMMAND ----------

import re
from pyspark.sql import functions as F

schema = "workspace.cits5504_project"
batch_id = "initial_load_001"

for file in dbutils.fs.ls(base_path):
    if not file.name.lower().endswith(".csv"):
        continue

    # Convert filename into a valid table name
    name = re.sub(r"[^a-z0-9]+", "_", file.name[:-4].lower()).strip("_")
    table_name = f"{schema}.bronze_{name}"

    if spark.catalog.tableExists(table_name):
        print(f"Skipped existing table: {table_name}")
        continue

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file.path)
    )

    # Standardise column names, preserving original data values
    column_names = [
        re.sub(r"[^a-z0-9]+", "_", col.lower()).strip("_")
        for col in df.columns
    ]
    df = df.toDF(*column_names)

    bronze_df = (
        df
        .withColumn("_source_file", F.lit(file.path))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("error")
        .saveAsTable(table_name)
    )

    print(f"Created: {table_name}")

# COMMAND ----------

# Validating fields in each csv files
for table in spark.catalog.listTables(schema):
    if table.name.startswith("bronze_"):
        table_name = f"{schema}.{table.name}"
        df = spark.table(table_name)

        print(f"\n{table.name}: {df.count():,} rows")
        print("Columns:", df.columns)

# COMMAND ----------

# Load the airline table
airline = spark.table("workspace.cits5504_project.bronze_airline_dataset_updated_v2")
airports = spark.table("workspace.cits5504_project.bronze_airports")
countries = spark.table("workspace.cits5504_project.bronze_countries")
navaids = spark.table("workspace.cits5504_project.bronze_navaids")
regions = spark.table("workspace.cits5504_project.bronze_regions")

# Preview the data
display(airline.limit(10))

# Count all records
print("Total records:", airline.count())

# Count missing passenger IDs
missing_ids = airline.filter(
    F.col("passenger_id").isNull()
    | (F.trim(F.col("passenger_id")) == "")
)

print("Missing passenger IDs:", missing_ids.count())

# Show repeated passenger IDs
repeated_ids = airline.groupBy("passenger_id").count()
repeated_ids = repeated_ids.filter(F.col("count") > 1)

display(repeated_ids)

# Count missing or placeholder airport codes
missing_airports = airline.filter(
    F.col("arrival_airport").isNull()
    | F.trim(F.col("arrival_airport")).isin("", "0")
)

print("Missing airport codes:", missing_airports.count())
# Show flight status values and their counts
display(airline.groupBy("flight_status").count())
# Inspect departure dates before converting them
display(airline.select("departure_date").distinct().limit(30))

# COMMAND ----------

display(airports.limit(10))
print("Total airports:", airports.count())

# Missing airport identifiers
missing_airports = airports.filter(
    F.col("ident").isNull()
    | (F.trim(F.col("ident")) == "")
)
print("Missing airport identifiers:", missing_airports.count())

# Repeated airport identifiers
display(
    airports.groupBy("ident")
    .count()
    .filter(F.col("count") > 1)
)

# Airport types
display(airports.groupBy("type").count())

# Missing IATA codes — these are not required for every airport
missing_iata = airports.filter(
    F.col("iata_code").isNull()
    | (F.trim(F.col("iata_code")) == "")
)
print("Airports without IATA codes:", missing_iata.count())

# COMMAND ----------

display(countries.limit(10))
print("Total countries:", countries.count())

# Missing country codes
missing_countries = countries.filter(
    F.col("code").isNull()
    | (F.trim(F.col("code")) == "")
)
print("Missing country codes:", missing_countries.count())

# Repeated country codes
display(
    countries.groupBy("code")
    .count()
    .filter(F.col("count") > 1)
)

# Countries by continent
display(countries.groupBy("continent").count())

# COMMAND ----------

display(regions.limit(10))
print("Total regions:", regions.count())

# Missing region codes
missing_regions = regions.filter(
    F.col("code").isNull()
    | (F.trim(F.col("code")) == "")
)
print("Missing region codes:", missing_regions.count())

# Repeated region codes
display(
    regions.groupBy("code")
    .count()
    .filter(F.col("count") > 1)
)

# Country codes that do not match the countries table
unmatched_regions = regions.join(
    countries,
    regions.iso_country == countries.code,
    "left_anti"
)

display(unmatched_regions.select("code", "name", "iso_country"))
print("Regions with unmatched country codes:", unmatched_regions.count())