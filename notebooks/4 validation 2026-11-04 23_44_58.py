# Databricks notebook source
schema = "workspace.cits5504_project"

tables = [
    "gold_dim_date",
    "gold_dim_passenger",
    "gold_dim_airport",
    "gold_dim_country",
    "gold_dim_region",
    "gold_dim_flight_status",
    "gold_dim_pilot",
    "gold_fact_passenger_record"
]

# Check every source exists before saving anything
for table in tables:
    assert spark.catalog.tableExists(f"{schema}.{table}"), \
        f"Missing source table: {schema}.{table}"

for table in tables:
    source = f"{schema}.{table}"
    baseline = f"{schema}.audit_before_{table}"

    # Keep the original baseline if this cell is run again
    if not spark.catalog.tableExists(baseline):
        spark.table(source).write \
            .format("delta") \
            .mode("error") \
            .saveAsTable(baseline)

        print(f"Baseline saved: {table}")
    else:
        print(f"Baseline already exists: {table}")

    print(
        f"  Current rows: {spark.table(source).count():,}"
        f" | Baseline rows: {spark.table(baseline).count():,}"
    )

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    ("gold_dim_date", "date_key", "departure_date"),
    ("gold_dim_passenger", "passenger_key", "passenger_id"),
    ("gold_dim_airport", "airport_key", "airport_ident"),
    ("gold_dim_country", "country_key", "country_code"),
    ("gold_dim_region", "region_key", "region_code"),
    ("gold_dim_flight_status", "flight_status_key", "flight_status"),
    ("gold_dim_pilot", "pilot_key", "pilot_name"),
    ("gold_fact_passenger_record", "fact_key", "passenger_key")
]

results = []

for table, key, natural_key in tables:
    current = spark.table(f"{schema}.{table}")
    baseline = spark.table(f"{schema}.audit_before_{table}")

    checks = current.selectExpr(
        "COUNT(*) AS total_rows",
        f"COUNT(DISTINCT {key}) AS unique_keys",
        f"COUNT(DISTINCT {natural_key}) AS unique_values"
    ).first()

    blank_ids = current.filter(
        F.trim(F.col(natural_key).cast("string")) == ""
    ).count()

    columns = baseline.columns

    added_or_changed = current.select(*columns).exceptAll(
        baseline.select(*columns)
    ).count()

    removed_or_changed = baseline.select(*columns).exceptAll(
        current.select(*columns)
    ).count()

    passed = (
        checks.total_rows == checks.unique_keys == checks.unique_values
        and blank_ids == 0
        and added_or_changed == 0
        and removed_or_changed == 0
    )

    results.append((
        table,
        checks.total_rows,
        checks.unique_keys,
        checks.unique_values,
        blank_ids,
        added_or_changed,
        removed_or_changed,
        "PASS" if passed else "FAIL"
    ))

report = spark.createDataFrame(
    results,
    [
        "table_name",
        "rows",
        "unique_surrogate_keys",
        "unique_record_identifiers",
        "blank_identifiers",
        "added_or_changed",
        "removed_or_changed",
        "result"
    ]
)

display(report.orderBy("table_name"))

assert all(row[-1] == "PASS" for row in results), \
    "Validation failed. Review the FAIL rows above."

print("PASS: all seven dimensions and the fact match their baselines.")