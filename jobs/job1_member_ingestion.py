# ============================================================
# CareFusion360 - Job1 (EMR SAFE VERSION)
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, row_number, sha2,
    current_date, year, current_timestamp
)
from pyspark.sql.window import Window

# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CareFusion360_Job1_Member_Pipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("🚀 Job1 Started")

# ============================================================
# 2. S3 PATHS
# ============================================================

historical_path = "s3a://carefusion360-data-lake/raw/s3/historical_members/"
migrated_path = "s3a://carefusion360-data-lake/raw/s3/migrated_members/"

output_path = "s3a://carefusion360-data-lake/staging/job1/"
quarantine_path = "s3a://carefusion360-data-lake/quarantine/job1/"

# ============================================================
# 3. READ DATA
# ============================================================

historical_df = spark.read.option("header", "true").csv(historical_path)
migrated_df = spark.read.option("header", "true").csv(migrated_path)

historical_df = historical_df.dropDuplicates()

# ============================================================
# 4. DATA VALIDATION
# ============================================================

current_year_col = year(current_date())

valid_df = historical_df.filter(
    (col("legacy_member_id").isNotNull()) &
    (col("effective_date").isNotNull()) &
    (col("birth_year").between(1900, 2025)) &
    (col("gender").isin("M", "F")) &
    (
        col("coverage_end_date").isNull() |
        (col("coverage_start_date") <= col("coverage_end_date"))
    )
)

quarantine_df = historical_df.join(
    valid_df.select("legacy_member_id", "effective_date"),
    on=["legacy_member_id", "effective_date"],
    how="left_anti"
)

print("Validation Done")

# ============================================================
# 5. WRITE QUARANTINE
# ============================================================

quarantine_df.withColumn("processing_date", current_date()) \
    .write.mode("append") \
    .partitionBy("processing_date") \
    .parquet(quarantine_path)

# ============================================================
# 6. SCD LOGIC
# ============================================================

window_spec = Window.partitionBy("legacy_member_id") \
    .orderBy(col("effective_date").desc(), col("record_updated_ts").desc())

scd_df = valid_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# ============================================================
# 7. JOIN
# ============================================================

joined_df = scd_df.join(migrated_df, on="legacy_member_id", how="inner")

# ============================================================
# 8. FEATURES
# ============================================================

joined_df = joined_df.withColumn("age", current_year_col - col("birth_year"))

joined_df = joined_df.withColumn(
    "age_band",
    when(col("age").between(18, 24), "18-24")
    .when(col("age").between(25, 34), "25-34")
    .when(col("age").between(35, 44), "35-44")
    .when(col("age").between(45, 54), "45-54")
    .otherwise("55+")
)

joined_df = joined_df.withColumn(
    "active_flag",
    when(
        col("coverage_end_date").isNull() |
        (col("coverage_end_date") > current_date()),
        "Active"
    ).otherwise("Terminated")
)

# ============================================================
# 9. MASKING
# ============================================================

joined_df = joined_df.withColumn("masked_ssn", sha2(col("ssn_raw"), 256))

joined_df = joined_df.drop(
    "patient_first_name", "patient_last_name", "email",
    "phone_number", "address_line", "city",
    "zip_code", "ssn_raw"
)

# ============================================================
# 10. MEMBER KEY
# ============================================================

joined_df = joined_df.withColumn("member_key", sha2(col("member_id"), 256))

# ============================================================
# 11. AUDIT
# ============================================================

final_df = joined_df.withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("record_status", lit("ACTIVE")) \
    .withColumn("dq_flag", lit("VALID")) \
    .withColumn("processing_date", current_date())

# ============================================================
# 12. SELECT
# ============================================================

final_df = final_df.select(
    "member_key", "member_id", "legacy_member_id",
    "age_band", "gender", "state", "enrollment_date",
    "policy_id", "policy_type", "coverage_start_date",
    "coverage_end_date", "member_segment", "active_flag",
    "migration_flag", "masked_ssn",
    "processing_timestamp", "record_status",
    "dq_flag", "processing_date"
)

# ============================================================
# 13. WRITE OUTPUT
# ============================================================

final_df.repartition(8).write \
    .mode("overwrite") \
    .partitionBy("processing_date", "active_flag") \
    .parquet(output_path)

print("✅ Job1 Completed Successfully")

spark.stop()