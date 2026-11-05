# Databricks notebook source
from pyspark.sql import functions as F

schema = "workspace.cits5504_project"
airline = spark.table(f"{schema}.bronze_airline_dataset_updated_v2")

silver_airline = airline.withColumnRenamed(
    "arrival_airport", "departure_airport_iata_code"
)

code = F.trim(F.col("departure_airport_iata_code"))

silver_airline = silver_airline.withColumn(
    "departure_airport_iata_code",
    F.when(code.isin("", "0"), F.lit(None)).otherwise(F.upper(code))
)

# COMMAND ----------

bronze_rows = airline.count()
silver_rows = silver_airline.count()
missing_codes = silver_airline.filter(
    F.col("departure_airport_iata_code").isNull()
).count()

print("Bronze rows:", bronze_rows)
print("Silver rows:", silver_rows)
print("Missing departure airport codes:", missing_codes)

assert silver_rows == bronze_rows, "Row count changed during cleaning"

# COMMAND ----------

#date inconsistencies
date_text = F.trim(F.col("departure_date"))

date_patterns = silver_airline.select(
    F.when(date_text.isNull() | (date_text == ""), "missing")
    .otherwise(F.regexp_replace(date_text, "[0-9]", "9"))
    .alias("date_pattern"),
    date_text.alias("date_example")
)

display(
    date_patterns.groupBy("date_pattern")
    .agg(
        F.count("*").alias("rows"),
        F.max("date_example").alias("example")
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

hyphen_dates = (
    silver_airline
    .filter(date_text.rlike(r"^\d{2}-\d{2}-\d{4}$"))
    .select(date_text.alias("date_text"))
)

parts = F.split(F.col("date_text"), "-")
first = parts.getItem(0).cast("int")
second = parts.getItem(1).cast("int")

date_order = (
    F.when((first > 12) & (second <= 12), "Day first")
    .when((second > 12) & (first <= 12), "Month first")
    .when((first <= 12) & (second <= 12), "Ambiguous")
    .otherwise("Check value")
)

display(
    hyphen_dates.withColumn("date_order", date_order)
    .groupBy("date_order")
    .agg(F.count("*").alias("rows"), F.min("date_text").alias("example"))
)

# COMMAND ----------

# DBTITLE 1,Parse departure dates
# Handle raw month-first dates and dates already converted to year-first
date_text = F.regexp_replace(
    F.trim(F.col("departure_date").cast("string")), "-", "/"
)

checked = silver_airline.withColumn("_date_text", date_text)
checked = checked.withColumn(
    "departure_date",
    F.expr("""
        CASE WHEN _date_text RLIKE '^[0-9]{4}/'
             THEN try_to_timestamp(_date_text, 'yyyy/M/d')
             ELSE try_to_timestamp(_date_text, 'M/d/yyyy')
        END
    """).cast("date")
)

# Show any dates that failed before saving
failed = checked.filter(F.col("departure_date").isNull())
display(failed.select("_date_text").distinct())
assert failed.count() == 0, "Review the displayed dates before saving"

silver_airline = checked.drop("_date_text")
(
    silver_airline.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{schema}.silver_airline")
)
silver_airline.select("departure_date").printSchema()

# COMMAND ----------

airports = spark.table(f"{schema}.bronze_airports")

# Standardise IATA codes before checking for duplicates
iata = F.upper(F.trim(F.col("iata_code")))
silver_airports = airports.withColumn(
    "iata_code",
    F.when(iata != "", iata)
)

# Show codes that appear on more than one airport row
repeated_codes = (
    silver_airports
    .filter(F.col("iata_code").isNotNull())
    .groupBy("iata_code")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"), "iata_code")
)

display(repeated_codes)

# COMMAND ----------

(
    silver_airports.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{schema}.silver_airports")
)

# COMMAND ----------

#JOIN
airline = spark.table(f"{schema}.silver_airline")

airport_lookup = spark.table(f"{schema}.silver_airports").select(
    F.col("iata_code").alias("departure_airport_iata_code"),
    F.col("ident").alias("airport_ident"),
    F.col("type").alias("airport_type"),
    F.col("iso_region").alias("airport_region")
)

airline_with_airports = airline.join(
    airport_lookup,
    on="departure_airport_iata_code",
    how="left"
)

# Check that the join preserved the airline row count
joined_rows = airline_with_airports.count()
print("Rows after join:", joined_rows)
assert joined_rows == airline.count(), "The join changed the row count"

# Find populated airline codes that have no match in the airport table
unmatched = airline_with_airports.filter(
    F.col("departure_airport_iata_code").isNotNull()
    & F.col("airport_ident").isNull()
)
print("Airline rows with an unmatched code:", unmatched.count())

display(
    airline_with_airports.select(
        "airport_name", "departure_airport_iata_code",
        "airport_ident", "airport_type", "airport_region"
    ).limit(10)
)

# COMMAND ----------

print(
    "Distinct unmatched codes:",
    unmatched.select("departure_airport_iata_code").distinct().count()
)

display(
    unmatched
    .groupBy("departure_airport_iata_code", "airport_name", "airport_country_code")
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
)

# COMMAND ----------

# Count how many reference airports have each name and country
reference_names = (
    silver_airports.select(
        F.lower(F.trim("name")).alias("name_key"),
        F.upper(F.trim("iso_country")).alias("country_key")
    )
    .groupBy("name_key", "country_key")
    .count()
    .withColumnRenamed("count", "reference_matches")
)

# Check each unmatched airline row against that lookup
name_check = (
    unmatched
    .withColumn("name_key", F.lower(F.trim("airport_name")))
    .withColumn("country_key", F.upper(F.trim("airport_country_code")))
    .join(reference_names, ["name_key", "country_key"], "left")
    .withColumn(
        "match_result",
        F.when(F.col("reference_matches").isNull(), "No exact match")
         .when(F.col("reference_matches") == 1, "One exact match")
         .otherwise("Multiple matches")
    )
)

display(name_check.groupBy("match_result").count())

# COMMAND ----------

# Keep only the name-and-country pairs that identified one reference airport
unique_keys = (
    name_check.filter(F.col("match_result") == "One exact match")
    .select("name_key", "country_key")
    .distinct()
)

name_lookup = (
    silver_airports
    .withColumn("name_key", F.lower(F.trim("name")))
    .withColumn("country_key", F.upper(F.trim("iso_country")))
    .join(unique_keys, ["name_key", "country_key"], "inner")
    .select("name_key", "country_key", "ident", "type", "iso_region")
)

# Recover airport details for the 3,677 rows
recovered = (
    name_check.filter(F.col("match_result") == "One exact match")
    .join(name_lookup, ["name_key", "country_key"], "inner")
    .withColumn("airport_ident", F.col("ident"))
    .withColumn("airport_type", F.col("type"))
    .withColumn("airport_region", F.col("iso_region"))
    .select(*airline_with_airports.columns)
    .withColumn("airport_match_method", F.lit("Name and country"))
)

# Combine the recovered rows with the other join outcomes
iata_matches = (
    airline_with_airports.filter(F.col("airport_ident").isNotNull())
    .withColumn("airport_match_method", F.lit("IATA code"))
)

unresolved = (
    name_check.filter(F.col("match_result") != "One exact match")
    .withColumn("airport_match_method", F.col("match_result"))
    .select(*airline_with_airports.columns, "airport_match_method")
)

missing_codes = (
    airline_with_airports.filter(F.col("departure_airport_iata_code").isNull())
    .withColumn("airport_match_method", F.lit("Missing code"))
)

silver_airline_enriched = (
    iata_matches.unionByName(recovered)
    .unionByName(unresolved)
    .unionByName(missing_codes)
)

assert silver_airline_enriched.count() == airline.count(), "Row count changed"
display(silver_airline_enriched.groupBy("airport_match_method").count())

# COMMAND ----------

missing_code_check = (
    missing_codes
    .withColumn("name_key", F.lower(F.trim("airport_name")))
    .withColumn("country_key", F.upper(F.trim("airport_country_code")))
    .join(reference_names, ["name_key", "country_key"], "left")
    .withColumn(
        "match_result",
        F.when(F.col("reference_matches") == 1, "One exact match")
         .when(F.col("reference_matches") > 1, "Multiple matches")
         .otherwise("No exact match")
    )
)

display(missing_code_check.groupBy("match_result").count())


# COMMAND ----------

# Find the reference airport for each unique name-and-country match
keys = ["name_key", "country_key"]

unique_missing_keys = (
    missing_code_check.filter(F.col("reference_matches") == 1)
    .select(*keys)
    .distinct()
)

missing_lookup = (
    silver_airports
    .withColumn("name_key", F.lower(F.trim("name")))
    .withColumn("country_key", F.upper(F.trim("iso_country")))
    .join(unique_missing_keys, keys, "inner")
    .select(*keys, "ident", "type", "iso_region")
)

# The left join keeps all 873 rows and fills details for the 659 unique matches
checked_missing = (
    missing_code_check
    .join(missing_lookup, keys, "left")
    .withColumn("airport_ident", F.col("ident"))
    .withColumn("airport_type", F.col("type"))
    .withColumn("airport_region", F.col("iso_region"))
    .withColumn(
        "airport_match_method",
        F.when(F.col("reference_matches") == 1, "Name and country")
         .otherwise(F.col("match_result"))
    )
    .select(*silver_airline_enriched.columns)
)

final_airline = (
    silver_airline_enriched
    .filter(F.col("airport_match_method") != "Missing code")
    .unionByName(checked_missing)
)

assert final_airline.count() == airline.count(), "Airline row count changed"
display(final_airline.groupBy("airport_match_method").count())

# COMMAND ----------

table_name = f"{schema}.silver_airline_enriched"
final_airline.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

#JOIN REGION
airline = spark.table(f"{schema}.silver_airline_enriched")
regions = spark.table(f"{schema}.bronze_regions")

region_lookup = regions.select(
    F.upper(F.trim("code")).alias("region_code"),
    F.trim("name").alias("region_name")
)

# Each code must identify at most one region before joining
duplicate_codes = region_lookup.groupBy("region_code").count().filter("count > 1")
assert duplicate_codes.count() == 0, "Repeated region codes found"


# COMMAND ----------

airline_geo = airline.join(
    region_lookup,
    F.upper(F.trim(airline["airport_region"])) == region_lookup["region_code"],
    "left"
)

assert airline_geo.count() == airline.count(), "The join changed the row count"

display(
    airline_geo.select(
        "airport_name", "country_name", "airport_region", "region_name"
    ).limit(20)
)

# Show region codes supplied by an airport but absent from the region lookup
unmatched_regions = airline_geo.filter(
    F.col("airport_region").isNotNull() & F.col("region_code").isNull()
)
display(unmatched_regions.groupBy("airport_region").count())

# COMMAND ----------

table_name = f"{schema}.silver_airline_geography"

(
    airline_geo.drop("region_code")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

saved_geo = spark.table(table_name)

display(
    saved_geo.agg(
        F.count("*").alias("total_rows"),
        F.count("region_name").alias("rows_with_region_name"),
        (F.count("*") - F.count("region_name")).alias("rows_without_region_name")
    )
)

# COMMAND ----------

countries = spark.table(f"{schema}.bronze_countries")

country_codes = countries.select(
    F.upper(F.trim("code")).alias("country_code")
).distinct()

country_check = (
    saved_geo
    .withColumn("country_code", F.upper(F.trim("airport_country_code")))
    .join(country_codes.withColumn("country_found", F.lit(True)), "country_code", "left")
)

display(
    country_check
    .filter(F.col("region_name").isNull())
    .groupBy(
        F.when(F.col("country_found").isNotNull(), "Country matched")
         .otherwise("Country missing or unmatched")
         .alias("country_result")
    )
    .count()
)