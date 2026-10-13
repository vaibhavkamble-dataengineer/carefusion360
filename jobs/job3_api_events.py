# ============================================================
# CareFusion360 - Job3 (EMR SAFE VERSION)
# ============================================================

import requests
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, lit, current_timestamp, to_date
)
from pyspark.sql.types import DoubleType

# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CareFusion360_Job3_WebAPI")
    .config("spark.sql.adaptive.enabled","true")
    .config("spark.sql.shuffle.partitions","200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Job3 Started")

# ============================================================
# 2. API INGESTION
# ============================================================

api_url = "https://carefusion-webapi-simulator.onrender.com/events"

max_retries = 3
retry_delay = 5

def fetch_events(x):
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, timeout=120)
            if response.status_code == 200 and response.text:
                return response.text
        except Exception as e:
            print("API error:", e)

        if attempt < max_retries - 1:
            time.sleep(retry_delay)

    raise Exception("API failed after retries")


api_calls = 3

api_rdd = spark.sparkContext.parallelize(range(api_calls), api_calls)
json_rdd = api_rdd.map(fetch_events)

raw_df = spark.read.json(json_rdd)

print("API Data Loaded")

# ============================================================
# 3. VALIDATION
# ============================================================

valid_df = (
    raw_df
    .filter(
        col("event_id").isNotNull() &
        col("member_id").isNotNull() &
        col("payload").isNotNull()
    )
    .dropDuplicates(["event_id"])
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_run_id", lit("EMR_RUN"))
    .withColumn("data_source", lit("webapi"))
    .withColumn("event_date", to_date("event_timestamp"))
)

valid_df = valid_df.repartition(4)

# ============================================================
# 4. CLAIMS DOMAIN
# ============================================================

claims_v2 = (
    valid_df
    .filter((col("event_category") == "CLAIMS") & (col("schema_version") == "v2"))
    .withColumn("claim_id", col("payload.claim.id"))
    .withColumn("claim_amount", col("payload.claim.amount").cast(DoubleType()))
    .withColumn("provider_id", col("payload.provider.id"))
    .withColumn("diagnosis_code", explode(col("payload.claim.diagnosis_codes")))
)

claims_v1 = (
    valid_df
    .filter((col("event_category") == "CLAIMS") & (col("schema_version") == "v1"))
    .withColumn("claim_id", col("payload.claim_id"))
    .withColumn("claim_amount", col("payload.claim_amount").cast(DoubleType()))
    .withColumn("provider_id", col("payload.provider_id"))
    .withColumn("diagnosis_code", lit(None))
)

claims_df = claims_v2.unionByName(claims_v1)

claims_valid = claims_df.filter(
    col("claim_id").isNotNull() &
    col("claim_amount").isNotNull() &
    (col("claim_amount") > 0) &
    col("provider_id").isNotNull()
)

claims_bad = claims_df.join(
    claims_valid.select("claim_id"),
    on="claim_id",
    how="left_anti"
)

# ============================================================
# 5. BILLING DOMAIN
# ============================================================

billing_v2 = (
    valid_df
    .filter((col("event_category") == "BILLING") & (col("schema_version") == "v2"))
    .withColumn("payment_amount", col("payload.payment.amount").cast(DoubleType()))
    .withColumn("payment_method", explode(col("payload.payment.methods")))
)

billing_v1 = (
    valid_df
    .filter((col("event_category") == "BILLING") & (col("schema_version") == "v1"))
    .withColumn("payment_amount", col("payload.payment_amount").cast(DoubleType()))
    .withColumn("payment_method", lit(None))
)

billing_df = billing_v2.unionByName(billing_v1)

billing_valid = billing_df.filter(
    col("payment_amount").isNotNull() &
    (col("payment_amount") > 0)
)

billing_bad = billing_df.join(
    billing_valid.select("payment_amount"),
    on="payment_amount",
    how="left_anti"
)

# ============================================================
# 6. POLICY DOMAIN
# ============================================================

policy_v2 = (
    valid_df
    .filter((col("event_category") == "POLICY") & (col("schema_version") == "v2"))
    .withColumn("policy_id", col("payload.policy.id"))
)

policy_v1 = (
    valid_df
    .filter((col("event_category") == "POLICY") & (col("schema_version") == "v1"))
    .withColumn("policy_id", col("payload.policy_id"))
)

policy_df = policy_v2.unionByName(policy_v1)

policy_valid = policy_df.filter(col("policy_id").isNotNull())

policy_bad = policy_df.join(
    policy_valid.select("policy_id"),
    on="policy_id",
    how="left_anti"
)

# ============================================================
# 7. MEMBER DOMAIN
# ============================================================

member_v2 = (
    valid_df
    .filter((col("event_category") == "MEMBER") & (col("schema_version") == "v2"))
    .withColumn("first_name", col("payload.profile.first_name"))
    .withColumn("last_name", col("payload.profile.last_name"))
)

member_v1 = (
    valid_df
    .filter((col("event_category") == "MEMBER") & (col("schema_version") == "v1"))
    .withColumn("first_name", col("payload.first_name"))
    .withColumn("last_name", col("payload.last_name"))
)

member_df = member_v2.unionByName(member_v1)

member_valid = member_df.filter(
    col("first_name").isNotNull() &
    col("last_name").isNotNull()
)

member_bad = member_df.join(
    member_valid.select("first_name"),
    on="first_name",
    how="left_anti"
)

# ============================================================
# 8. WRITE OUTPUT
# ============================================================

base_path = "s3a://carefusion360-data-lake/staging/job3"
quarantine_path = "s3a://carefusion360-data-lake/quarantine/job3"

claims_valid.write.mode("overwrite").partitionBy("event_date").parquet(f"{base_path}/claims")
billing_valid.write.mode("overwrite").partitionBy("event_date").parquet(f"{base_path}/billing")
policy_valid.write.mode("overwrite").partitionBy("event_date").parquet(f"{base_path}/policy")
member_valid.write.mode("overwrite").partitionBy("event_date").parquet(f"{base_path}/member")

claims_bad.write.mode("overwrite").parquet(f"{quarantine_path}/claims_bad")
billing_bad.write.mode("overwrite").parquet(f"{quarantine_path}/billing_bad")
policy_bad.write.mode("overwrite").parquet(f"{quarantine_path}/policy_bad")
member_bad.write.mode("overwrite").parquet(f"{quarantine_path}/member_bad")

print("Job3 Completed Successfully")

spark.stop()
