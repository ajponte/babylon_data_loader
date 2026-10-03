#!/usr/bin/env bash
# ==============================================================================
# deploy-lambda.sh: Update AWS Lambda Container Image and Wait for Completion
#
# Usage:
#   REGISTRY=615471835001.dkr.ecr.us-west-2.amazonaws.com \
#   ECR_REPOSITORY=ajp/babylon \
#   LAMBDA_FUNCTION_NAME=babylon-data-loader \
#   DEPLOY_TAG=data-loader-sha-9035067 \
#   AWS_REGION=us-west-2 \
#   ./deploy-lambda.sh
# ==============================================================================
set -euo pipefail

REGISTRY="${REGISTRY:?REGISTRY is required}"
ECR_REPOSITORY="${ECR_REPOSITORY:?ECR_REPOSITORY is required}"
LAMBDA_FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:?LAMBDA_FUNCTION_NAME is required}"
DEPLOY_TAG="${DEPLOY_TAG:?DEPLOY_TAG is required}"
AWS_REGION="${AWS_REGION:-us-west-2}"

IMAGE_URI="${REGISTRY}/${ECR_REPOSITORY}:${DEPLOY_TAG}"

echo "============================================================"
echo " Starting AWS Lambda Deployment"
echo " Function:   ${LAMBDA_FUNCTION_NAME}"
echo " Image URI:  ${IMAGE_URI}"
echo " Region:     ${AWS_REGION}"
echo "============================================================"

# 1. Verify target function exists
echo "==> Verifying target Lambda function existence..."
aws lambda get-function-configuration \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --query '{FunctionName:FunctionName,State:State,LastUpdateStatus:LastUpdateStatus,CurrentCodeSha:CodeSha256}' \
  --output table

# 2. Update function code
echo "==> Updating Lambda function code..."
UPDATE_OUTPUT=$(aws lambda update-function-code \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --image-uri "${IMAGE_URI}" \
  --region "${AWS_REGION}")

REVISION_ID=$(echo "${UPDATE_OUTPUT}" | jq -r '.RevisionId')
echo "==> Update triggered successfully (RevisionId: ${REVISION_ID})."

# 3. Wait for update to complete
echo "==> Waiting for Lambda function update to complete..."
aws lambda wait function-updated \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}"

# 4. Verify post-deployment status
echo "==> Verifying post-deployment configuration..."
FINAL_STATUS=$(aws lambda get-function-configuration \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}")

LAST_UPDATE_STATUS=$(echo "${FINAL_STATUS}" | jq -r '.LastUpdateStatus')
FUNCTION_STATE=$(echo "${FINAL_STATUS}" | jq -r '.State')
DEPLOYED_SHA=$(echo "${FINAL_STATUS}" | jq -r '.CodeSha256')

if [ "${LAST_UPDATE_STATUS}" != "Successful" ]; then
  echo "ERROR: Lambda update status is '${LAST_UPDATE_STATUS}', expected 'Successful'!" >&2
  exit 1
fi

if [ "${FUNCTION_STATE}" != "Active" ]; then
  echo "ERROR: Lambda state is '${FUNCTION_STATE}', expected 'Active'!" >&2
  exit 1
fi

echo "============================================================"
echo " Lambda Deployment Successful!"
echo " Function:   ${LAMBDA_FUNCTION_NAME}"
echo " State:      ${FUNCTION_STATE}"
echo " Status:     ${LAST_UPDATE_STATUS}"
echo " CodeSha256: ${DEPLOYED_SHA}"
echo "============================================================"
