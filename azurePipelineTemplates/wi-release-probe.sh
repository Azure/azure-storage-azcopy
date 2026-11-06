#!/usr/bin/env bash
# TEMPORARY (do not merge): workload identity regression check.
# Runs a released AzCopy binary with AZCOPY_AUTO_LOGIN_TYPE=WORKLOAD, wired the way the
# AKS workload identity webhook wires a pod, using the service connection's federated token.
set -uo pipefail

: "${AZCOPY_PROBE_BIN:?}" "${AZCOPY_PROBE_VERSION:?}"
: "${AZCOPY_PROBE_ACCOUNT_NAME:?}" "${AZCOPY_PROBE_ACCOUNT_KEY:?}"
: "${idToken:?}" "${servicePrincipalId:?}" "${tenantId:?}"

work="$(mktemp -d)"
token_file="$work/azure-identity-token"
(umask 077 && printf '%s' "$idToken" > "$token_file")

container="wiprobe-${AZCOPY_PROBE_VERSION//./-}-${BUILD_BUILDID:-local}-${SYSTEM_JOBATTEMPT:-1}"
blob_base="https://${AZCOPY_PROBE_ACCOUNT_NAME}.blob.core.windows.net/${container}"

container_cmd() {
  az storage container "$1" --name "$container" --account-name "$AZCOPY_PROBE_ACCOUNT_NAME" \
    --account-key "$AZCOPY_PROBE_ACCOUNT_KEY" --output none 2>/dev/null ||
    az storage container "$1" --name "$container" --account-name "$AZCOPY_PROBE_ACCOUNT_NAME" \
      --auth-mode login --output none
}

cleanup() {
  container_cmd delete || echo "##vso[task.logissue type=warning]Could not delete container $container"
  rm -rf "$work"
}
trap cleanup EXIT

container_cmd create || { echo "##vso[task.logissue type=error]Could not create probe container $container"; exit 1; }
printf 'workload identity probe\n' > "$work/probe.txt"

echo "Agent check: Entra discovery for the service connection tenant returns HTTP $(curl -s -o /dev/null -w '%{http_code}' "https://login.microsoftonline.com/${tenantId}/v2.0/.well-known/openid-configuration")"
"$AZCOPY_PROBE_BIN" --version

auth_failures=0
other_failures=0
summary=()

probe() {
  local label="$1" authority="$2"
  local logs="$work/logs-$label" plans="$work/plans-$label" home="$work/home-$label"
  mkdir -p "$logs" "$plans" "$home"

  # Start from an empty environment so only the variables a workload identity pod would have are present.
  local -a envs=(
    "PATH=$PATH"
    "HOME=$home"
    "AZCOPY_AUTO_LOGIN_TYPE=WORKLOAD"
    "AZURE_CLIENT_ID=$servicePrincipalId"
    "AZURE_TENANT_ID=$tenantId"
    "AZURE_FEDERATED_TOKEN_FILE=$token_file"
    "AZCOPY_LOG_LOCATION=$logs"
    "AZCOPY_JOB_PLAN_LOCATION=$plans"
    "AZCOPY_SHOW_PERF_STATES=true"
    "AZCOPY_TELEMETRY_CONNECTION_STRING=InstrumentationKey=00000000-0000-0000-0000-000000000000"
  )
  if [[ -n "$authority" ]]; then
    envs+=("AZURE_AUTHORITY_HOST=$authority")
  fi

  echo "##[group]AzCopy $AZCOPY_PROBE_VERSION [$label] AZURE_AUTHORITY_HOST=${authority:-<unset>}"
  local out rc result
  out="$(env -i "${envs[@]}" "$AZCOPY_PROBE_BIN" copy "$work/probe.txt" "$blob_base/$label.txt" 2>&1)"
  rc=$?
  echo "$out"

  if [[ $rc -eq 0 ]]; then
    result="PASS"
  elif grep -qiE 'WorkloadIdentityCredential|auto-?login' <<<"$out"; then
    result="AUTH_FAIL"
    auth_failures=$((auth_failures + 1))
  else
    result="OTHER_FAIL"
    other_failures=$((other_failures + 1))
  fi

  if [[ $result != "PASS" ]]; then
    echo "--- AzCopy log tail ---"
    tail -n 80 "$logs"/*.log 2>/dev/null || echo "(no AzCopy log written)"
  fi
  echo "##[endgroup]"
  summary+=("AzCopy $AZCOPY_PROBE_VERSION [$label] exit=$rc result=$result")
}

probe "authority-unset" ""
probe "authority-aks-webhook" "https://login.microsoftonline.com/"

echo "===== Summary ====="
printf '%s\n' "${summary[@]}"

if ((auth_failures > 0)); then
  echo "##vso[task.logissue type=error]AzCopy $AZCOPY_PROBE_VERSION failed WORKLOAD auto-login in $auth_failures variant(s)"
  exit 1
fi
if ((other_failures > 0)); then
  echo "##vso[task.logissue type=warning]AzCopy $AZCOPY_PROBE_VERSION authenticated but the transfer failed in $other_failures variant(s)"
  echo "##vso[task.complete result=SucceededWithIssues;]"
fi
exit 0
