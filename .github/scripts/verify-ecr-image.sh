#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${REGISTRY:-}"
ECR_REPOSITORY="${ECR_REPOSITORY:?ECR_REPOSITORY is required}"
IMAGE_TAGS="${IMAGE_TAGS:?IMAGE_TAGS is required}"
AWS_REGION="${AWS_REGION:-us-east-1}"

FIRST_TAG=$(echo "${IMAGE_TAGS}" | cut -d',' -f1 | xargs)
FULL_TARGET="${REGISTRY:+${REGISTRY}/}${ECR_REPOSITORY}:${FIRST_TAG}"

echo "Verifying manifest for ${FULL_TARGET} in region ${AWS_REGION}..."
aws ecr describe-images \
  --repository-name "${ECR_REPOSITORY}" \
  --image-ids imageTag="${FIRST_TAG}" \
  --region "${AWS_REGION}" \
  --query 'imageDetails[0].{Digest:imageDigest,Tags:imageTags,PushedAt:imagePushedAt,Size:imageSizeInBytes}' \
  --output table
