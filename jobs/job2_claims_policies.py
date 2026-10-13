# ============================================================
# CareFusion360 - Job2 (FINAL FIXED VERSION)
# Snowflake Claims Domain Pipeline
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, current_timestamp, lit, year, broadcast
)

import json
import subprocess

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CareFusion360_Job2_Claims_Pipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Job2 Started")

# ============================================================
# 2. FETCH SNOWFLAKE CREDENTIALS (SECURE CLI)
# ============================================================

secret_name = "snowp"
region_name = "ap-south-1"

try:
    result = subprocess.run(
        [
            "aws", "secretsmanager", "get-secret-value",
            "--secret-id", secret_name,
            "--region", region_name
        ],
        capture_output=True,
        text=True,
        check=True
    )

    secret_dict = json.loads(result.stdout)

    if "SecretString" not in secret_dict:
        raise ValueError("SecretString not found")

    secret = json.loads(secret_dict["SecretString"])

    print("Secret fetched successfully")

except subprocess.CalledProcessError as e:
    print("AWS CLI failed:")
    print(e.stderr)
    raise

except Exception as e:
    print("Secret parsing failed:", str(e))
    raise

# ============================================================
# 3. SNOWFLAKE OPTIONS (FIXED)
# ============================================================

sfOptions = {
    "sfURL": f"{secret['sfAccount']}.snowflakecomputing.com",
    "sfUser": secret["sfUser"],
    "sfPassword": secret["sfPassword"],
    "sfDatabase": secret["sfDatabase"],
    "sfSchema": secret["sfSchema"],
    "sfWarehouse": secret["sfWarehouse"],
    "tempDir": "s3://carefusion360-data-lake/tmp/snowflake/"
}

print("Snowflake config ready")

# ============================================================
# 4. S3 PATHS
# ============================================================

output_base = "s3a://carefusion360-data-lake/staging/job2"
quarantine_base = "s3a://carefusion360-data-lake/quarantine/job2"

claims_output = f"{output_base}/claims_fact"
revenue_output = f"{output_base}/revenue"
violations_output = f"{output_base}/violations"

bad_claims_path = f"{quarantine_base}/bad_claims"
bad_adjustments_path = f"{quarantine_base}/bad_adjustments"

# ============================================================
# 5. READ DATA FROM SNOWFLAKE
# ============================================================

print("Reading Snowflake tables...")

members_df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CARE_FUSION_DB.RAW.MEMBERS_CURRENT") \
    .load()

claims_df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CARE_FUSION_DB.RAW.CLAIMS") \
    .load()

adjust_df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CARE_FUSION_DB.RAW.CLAIMS_ADJUSTMENTS") \
    .load()

revenue_df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CARE_FUSION_DB.RAW.REVENUE") \
    .load()

violations_df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CARE_FUSION_DB.RAW.VIOLATIONS") \
    .load()

print("Snowflake data loaded")

# ============================================================
# 6. CLAIM VALIDATION
# ============================================================

claims_df = claims_df.dropDuplicates()

valid_claims = (
    claims_df
    .join(broadcast(members_df.select("member_id")), on="member_id", how="inner")
    .filter(
        (col("claim_id").isNotNull()) &
        (col("claim_date").isNotNull()) &
        (col("claim_amount") >= 0) &
        (col("claim_amount") <= 75000)
    )
)

bad_claims = claims_df.join(
    valid_claims.select("claim_id"),
    on="claim_id",
    how="left_anti"
)

bad_claims.write.mode("overwrite").parquet(bad_claims_path)

print("Claim validation done")

# ============================================================
# 7. ADJUSTMENT VALIDATION
# ============================================================

adjust_with_claim = adjust_df.join(
    valid_claims.select("claim_id", "claim_date"),
    on="claim_id",
    how="inner"
)

valid_adjust = adjust_with_claim.filter(
    (col("original_claim_amount") + col("adjustment_amount")
     == col("adjusted_final_amount")) &
    (col("adjustment_date") >= col("claim_date"))
).drop("claim_date")

bad_adjust = adjust_df.join(
    valid_adjust.select("adjustment_id"),
    on="adjustment_id",
    how="left_anti"
)

bad_adjust.write.mode("overwrite").parquet(bad_adjustments_path)

print("Adjustment validation done")

# ============================================================
# 8. BUILD CLAIM FACT
# ============================================================

claims_fact = valid_claims.join(
    valid_adjust,
    on=["claim_id", "member_id"],
    how="left"
)

claims_fact = claims_fact.withColumn(
    "final_claim_amount",
    when(col("adjusted_final_amount").isNull(), col("claim_amount"))
    .otherwise(col("adjusted_final_amount"))
)

claims_fact = claims_fact.withColumn(
    "processing_timestamp", current_timestamp()
).withColumn(
    "dq_flag", lit("VALID")
)

claims_fact = claims_fact.withColumn(
    "claim_year", year(col("claim_date"))
)

# ============================================================
# 9. CLEAN REVENUE
# ============================================================

revenue_clean = revenue_df.filter(col("premium_collected") >= 0) \
    .withColumn("processing_timestamp", current_timestamp())

# ============================================================
# 10. CLEAN VIOLATIONS
# ============================================================

violations_clean = violations_df.dropDuplicates() \
    .withColumn("processing_timestamp", current_timestamp())

# ============================================================
# 11. WRITE TO S3
# ============================================================

claims_fact = claims_fact.repartition(8)

claims_fact.write.mode("overwrite") \
    .partitionBy("claim_year") \
    .parquet(claims_output)

revenue_clean.write.mode("overwrite").parquet(revenue_output)

violations_clean.write.mode("overwrite").parquet(violations_output)

print("Job2 Completed Successfully")

spark.stop()
