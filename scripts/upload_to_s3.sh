#!/usr/bin/env bash
set -euo pipefail
BUCKET="${CAREFUSION_CODE_BUCKET:-carefusion360-pipeline-code}"
aws s3 cp ./jobs "s3://${BUCKET}/jobs/" --recursive
aws s3 cp ./configs "s3://${BUCKET}/configs/" --recursive
aws s3 cp ./dependencies "s3://${BUCKET}/dependencies/" --recursive
aws s3 cp ./utils "s3://${BUCKET}/utils/" --recursive
echo "Pipeline code uploaded to ${BUCKET}."
