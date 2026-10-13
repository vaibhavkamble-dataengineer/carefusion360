# ============================================================
# CareFusion360 - Job4 (FINAL EMR VERSION - CLI + SECURE)
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, max as spark_max,
    datediff, current_date, when, lit, log, current_timestamp
)

import subprocess
import json

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CareFusion360_Job4_Member360_Gold")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Started")

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
    secret = json.loads(secret_dict["SecretString"])

except Exception as e:
    print(f"Secret fetch failed: {str(e)}")
    raise

# ============================================================
# 3. SNOWFLAKE OPTIONS
# ============================================================

sfOptions = {
    "sfURL": f"{secret['sfAccount']}.snowflakecomputing.com",
    "sfUser": secret["sfUser"],
    "sfPassword": secret["sfPassword"],
    "sfDatabase": "CARE_FUSION_DB",
    "sfSchema": "CURATED",
    "sfWarehouse": secret["sfWarehouse"],
    "tempDir": "s3://carefusion360-data-lake/tmp/snowflake/"
}

# ============================================================
# 4. PATHS
# ============================================================

BASE_PATH = "s3a://carefusion360-data-lake/staging"
OUTPUT_PATH = "s3a://carefusion360-data-lake/curated/member_360"

# ============================================================
# 5. LOAD DATA
# ============================================================

member_dim = (
    spark.read.parquet(f"{BASE_PATH}/job1")
    .filter(col("active_flag") == "Active")
)

claims_df = spark.read.parquet(f"{BASE_PATH}/job2/claims_fact")
revenue_df = spark.read.parquet(f"{BASE_PATH}/job2/revenue")
violations_df = spark.read.parquet(f"{BASE_PATH}/job2/violations")

# ============================================================
# 6. AGGREGATIONS
# ============================================================

claims_agg = claims_df.groupBy("member_id").agg(
    spark_sum("final_claim_amount").alias("lifetime_claim_amount"),
    count("claim_id").alias("total_claim_count")
)

revenue_agg = revenue_df.groupBy("member_id").agg(
    spark_sum("premium_collected").alias("lifetime_revenue")
)

violations_agg = violations_df.groupBy("member_id").agg(
    count("violation_id").alias("violation_count")
)

# ============================================================
# 7. JOB3 STREAM DATA
# ============================================================

job3_claims = spark.read.parquet(f"{BASE_PATH}/job3/claims")
job3_billing = spark.read.parquet(f"{BASE_PATH}/job3/billing")
job3_policy = spark.read.parquet(f"{BASE_PATH}/job3/policy")
job3_member = spark.read.parquet(f"{BASE_PATH}/job3/member")

claims_stream_agg = job3_claims.groupBy("member_id").agg(
    count("event_id").alias("streaming_claim_events"),
    spark_max("event_id").alias("last_claim_event_id")
)

billing_stream_agg = job3_billing.groupBy("member_id").agg(
    count("event_id").alias("streaming_payment_events")
)

policy_stream_agg = job3_policy.groupBy("member_id").agg(
    count("event_id").alias("policy_change_count")
)

member_stream_agg = job3_member.groupBy("member_id").agg(
    count("event_id").alias("member_update_count")
)

# ============================================================
# 8. JOIN ALL
# ============================================================

member_360 = (
    member_dim
    .join(claims_agg, "member_id", "left")
    .join(revenue_agg, "member_id", "left")
    .join(violations_agg, "member_id", "left")
    .join(claims_stream_agg, "member_id", "left")
    .join(billing_stream_agg, "member_id", "left")
    .join(policy_stream_agg, "member_id", "left")
    .join(member_stream_agg, "member_id", "left")
)

# Safe fillna only for numeric columns
member_360 = member_360.fillna({
    "lifetime_claim_amount": 0,
    "lifetime_revenue": 0,
    "total_claim_count": 0,
    "violation_count": 0,
    "streaming_claim_events": 0,
    "streaming_payment_events": 0,
    "policy_change_count": 0,
    "member_update_count": 0
})

# ============================================================
# 9. DERIVED METRICS
# ============================================================

member_360 = member_360.withColumn(
    "tenure_days", datediff(current_date(), col("enrollment_date"))
)

member_360 = member_360.withColumn(
    "lifetime_value", col("lifetime_revenue") - col("lifetime_claim_amount")
)

member_360 = member_360.withColumn(
    "engagement_score",
    log(col("streaming_claim_events") + col("streaming_payment_events") + lit(1))
)

member_360 = member_360.withColumn(
    "risk_tier",
    when((col("violation_count") > 5) | (col("lifetime_claim_amount") > 100000), "HIGH")
    .when(col("violation_count") > 0, "MEDIUM")
    .otherwise("LOW")
)

member_360 = member_360.withColumn(
    "churn_flag",
    when(
        (col("streaming_claim_events") == 0) &
        (col("streaming_payment_events") == 0),
        "HIGH_CHURN_RISK"
    ).otherwise("STABLE")
)

member_360 = member_360.withColumn(
    "processing_timestamp", current_timestamp()
)

# ============================================================
# 10. FINAL SELECT
# ============================================================

final_df = member_360.select(
    "member_id", "member_key", "age_band", "state",
    "tenure_days", "lifetime_claim_amount", "lifetime_revenue",
    "total_claim_count", "violation_count",
    "streaming_claim_events", "streaming_payment_events",
    "policy_change_count", "member_update_count",
    "engagement_score", "lifetime_value",
    "risk_tier", "churn_flag", "processing_timestamp"
)

print("Records to write:", final_df.count())

# ============================================================
# 11. WRITE TO S3
# ============================================================

final_df.repartition(8, "state") \
    .write.mode("append") \
    .partitionBy("state") \
    .parquet(OUTPUT_PATH)

print("Written to S3")

# ============================================================
# 12. WRITE TO SNOWFLAKE
# ============================================================

final_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "MEMBER_360") \
    .mode("append") \
    .save()

print("Written to Snowflake")

spark.stop()
