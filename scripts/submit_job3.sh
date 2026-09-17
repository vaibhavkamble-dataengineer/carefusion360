#!/usr/bin/env bash
set -euo pipefail
BUCKET="${CAREFUSION_CODE_BUCKET:-carefusion360-pipeline-code}"
spark-submit --deploy-mode cluster --py-files "s3://${BUCKET}/dependencies/dependencies.zip" "s3://${BUCKET}/jobs/job3_api_events.py"
