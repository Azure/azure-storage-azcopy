ver=`../azcopy --version | cut -d " " -f 3`
image="azure-azcopy-$3.$ver"

sudo docker login azcopycontainers.azurecr.io --username $1 --password $2

# Publish Ubn-22 container image
sudo docker tag $image:latest azcopycontainers.azurecr.io/$image
sudo docker push azcopycontainers.azurecr.io/$image

sudo docker logout azcopycontainers.azurecr.io



#!/usr/bin/env bash
set -euo pipefail

# Args:
#   $1 = ACR username
#   $2 = ACR password
#   $3 = image variant suffix (example: ubuntu-x86_64)

REGISTRY="azcopycontainers.azurecr.io"
VER=$(../azcopy --version | cut -d " " -f 3)
IMAGE="azure-azcopy-$3.$VER"
LOCAL_TAG="$IMAGE:latest"
REMOTE_TAG="$REGISTRY/$IMAGE"

echo "Building/publishing image: $LOCAL_TAG"
echo "Remote target: $REMOTE_TAG"

# -----------------------------
# 1) Pre-push security gate
# -----------------------------
# Requires Defender for Cloud CLI to be installed and authenticated.
# --defender-break causes a non-zero exit code if critical issues are found.
echo "Running pre-push security scan..."
defender scan image "$LOCAL_TAG" \
  --defender-break \ 
  --defender-output "./defender-scan-$IMAGE.sarif"

echo "Pre-push scan passed."

# -----------------------------
# 2) Login to ACR
# -----------------------------
echo "Logging into ACR..."
sudo docker login "$REGISTRY" --username "$1" --password "$2"

# -----------------------------
# 3) Tag and push
# -----------------------------
echo "Tagging image..."
sudo docker tag "$LOCAL_TAG" "$REMOTE_TAG"

echo "Pushing image to ACR..."
sudo docker push "$REMOTE_TAG"

# -----------------------------
# 4) Informational post-push note
# -----------------------------
# Defender for Cloud scans images in ACR on push/import/pull-based triggers.
# This script cannot directly guarantee scan completion unless your pipeline
# separately queries Defender results or gates deployment on them.
echo "Image pushed successfully: $REMOTE_TAG"
echo "ACR-side vulnerability scanning should now be triggered if Defender for Cloud is enabled for this subscription."


QUERY="
securityresources
| where type == 'microsoft.security/assessments/subassessments'
| where properties.resourceDetails.type =~ 'ContainerRegistry'
| extend repo = tostring(properties.additionalData.artifactDetails.repositoryName)
| extend registry = tostring(properties.additionalData.artifactDetails.registryHost)
| extend severity = tostring(properties.status.severity)
| where repo contains '$IMAGE'
| project repo, registry, severity
"

MAX_RETRIES=10
SLEEP=30

echo "Waiting for Defender scan results..."

for i in $(seq 1 $MAX_RETRIES); do
  echo "Attempt $i..."

  result=$(az graph query -q "$QUERY" -o tsv)

  if [[ -n "$result" ]]; then
    echo "✅ Scan results available"
    echo "$result"
    break
  fi

  echo "⏳ No results yet (scan still running)..."
  sleep $SLEEP
done

# ✅ If still empty after retries → treat as failure
if [[ -z "$result" ]]; then
  echo "❌ Scan did not complete in time"
  exit 1
fi

# ✅ Fail on bad severity
if echo "$result" | grep -E "Critical|High"; then
  echo "❌ Vulnerabilities found"
  exit 1
fi

echo "✅ Scan passed"