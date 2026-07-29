"""
F1 Project | cleaning.py
Per-dataset PySpark cleaning (Bronze DataFrame -> Silver DataFrame).

NOTE: Jolpica/Ergast IDs (driverId, constructorId) are STRING slugs
(e.g. "max_verstappen", "ferrari"), not integers - so id columns are
cast to string here, not int.
"""
from pyspark.sql import functions as F


def _to_date(col):
    return F.date_format(F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col, "MM-dd-yyyy")), "yyyy-MM-dd")


def clean_drivers(df):
    return (df
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("driver_name", F.initcap(F.trim(
            F.concat_ws(" ", F.col("given_name"), F.col("family_name")))))
        .withColumn("driver_code", F.upper(F.trim(F.col("code"))))
        .withColumn("driver_number", F.col("number").cast("int"))
        .withColumn("nationality", F.initcap(F.trim(F.col("nationality"))))
        .withColumn("date_of_birth", _to_date(F.col("date_of_birth")))
        .filter(F.col("driver_id").isNotNull())
        .dropDuplicates(["driver_id"])
        .select("driver_id", "driver_name", "driver_code", "driver_number",
                "nationality", "date_of_birth"))


def clean_constructors(df):
    return (df
        .withColumn("constructor_id", F.col("constructor_id").cast("string"))
        .withColumn("constructor_name", F.initcap(F.trim(F.col("name"))))
        .withColumn("nationality", F.initcap(F.trim(F.col("nationality"))))
        .filter(F.col("constructor_id").isNotNull())
        .dropDuplicates(["constructor_id"])
        .select("constructor_id", "constructor_name", "nationality"))


def clean_races(df):
    return (df
        .withColumn("race_id", F.col("race_id").cast("string"))
        .withColumn("season", F.col("season").cast("int"))
        .withColumn("round", F.col("round").cast("int"))
        .withColumn("race_name", F.initcap(F.trim(F.col("race_name"))))
        .withColumn("circuit_name", F.initcap(F.trim(F.col("circuit_name"))))
        .withColumn("race_date", _to_date(F.col("date")))
        .filter(F.col("race_id").isNotNull())
        .dropDuplicates(["race_id"])
        .select("race_id", "season", "round", "race_name", "circuit_name", "race_date"))


def clean_results(df):
    return (df
        .withColumn("race_id", F.col("race_id").cast("string"))
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("constructor_id", F.col("constructor_id").cast("string"))
        .withColumn("grid", F.col("grid").cast("int"))
        .withColumn("position", F.regexp_replace(F.col("position").cast("string"), "[^0-9]", "").cast("int"))
        .withColumn("points", F.col("points").cast("double"))
        .withColumn("status", F.trim(F.col("status")))
        .filter(F.col("race_id").isNotNull() & F.col("driver_id").isNotNull())
        .dropDuplicates(["race_id", "driver_id"])
        .select("race_id", "driver_id", "constructor_id", "grid",
                "position", "points", "status"))


CLEANERS = {
    "drivers": clean_drivers,
    "constructors": clean_constructors,
    "races": clean_races,
    "results": clean_results,
}