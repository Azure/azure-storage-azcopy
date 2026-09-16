#!/usr/bin/env bash
set -euo pipefail
root="${AZCOPY_WSL_PERF_ROOT:?Set a Linux filesystem directory containing bin/azcopy and bin/azcopy.test}"
: "${WSL_AZURE_CONFIG_DIR:?Set the existing Windows Azure CLI config directory as a WSL path}"
: "${AZCOPY_LIVE_TELEMETRY_SUBSCRIPTION:?Set the test subscription}"
: "${AZCOPY_LIVE_TELEMETRY_RESOURCE_GROUP:?Set the isolated telemetry resource group}"
: "${AZCOPY_LIVE_TELEMETRY_SUFFIX:?Set the isolated telemetry resource suffix}"
: "${AZCOPY_LIVE_TELEMETRY_STORAGE_ACCOUNT:?Set the Blob test account}"
: "${AZCOPY_LIVE_TELEMETRY_OUTPUT:?Set a fresh evidence directory on the Linux filesystem}"
: "${AZCOPY_WSL_GRANT_STORAGE_PERMISSION:?Set to 1 to authorize a temporary container-scoped role, or 0 to use existing permissions}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$root/bin" "$root/tmp" "$AZCOPY_LIVE_TELEMETRY_OUTPUT"
if [[ -e "$AZCOPY_LIVE_TELEMETRY_OUTPUT/run.log" ]]; then
    printf 'Refusing to overwrite existing benchmark output\n' >&2
    exit 1
fi
if [[ -L "$root/bin/az" ]]; then
    unlink "$root/bin/az"
fi
cp "$script_dir/telemetry-wsl-az.sh" "$root/bin/az"
chmod u+x "$root/bin/az"
export PATH="$root/bin:$PATH"
export TMPDIR="$root/tmp"
export AZCOPY_RUN_CLI_TELEMETRY_PERF=1 AZCOPY_RUN_LIVE_TELEMETRY=1 AZCOPY_DISABLE_TELEMETRY=true
export AZCOPY_CLI_TELEMETRY_PERF_WORKLOAD=large-only
export AZCOPY_CLI_TELEMETRY_PERF_PAIRS="${AZCOPY_CLI_TELEMETRY_PERF_PAIRS:-3}"
export AZCOPY_CLI_TELEMETRY_PROFILE="${AZCOPY_CLI_TELEMETRY_PROFILE:-0}"
if [[ ! "$AZCOPY_CLI_TELEMETRY_PERF_PAIRS" =~ ^([3-9]|1[0-5])$ ]]; then
    printf 'Performance pairs must be 3..15\n' >&2
    exit 1
fi
export AZCOPY_LIVE_TELEMETRY_EXECUTABLE="$root/bin/azcopy"
if [[ "$AZCOPY_WSL_GRANT_STORAGE_PERMISSION" == 1 ]]; then
    AZCOPY_LIVE_TELEMETRY_STORAGE_PRINCIPAL="$(az ad signed-in-user show --query id -o tsv --only-show-errors | tr -d '\r')"
    AZCOPY_LIVE_TELEMETRY_STORAGE_RESOURCE_ID="$(az resource list --subscription "$AZCOPY_LIVE_TELEMETRY_SUBSCRIPTION" --name "$AZCOPY_LIVE_TELEMETRY_STORAGE_ACCOUNT" --resource-type Microsoft.Storage/storageAccounts --query '[0].id' -o tsv --only-show-errors | tr -d '\r')"
    export AZCOPY_LIVE_TELEMETRY_STORAGE_PRINCIPAL AZCOPY_LIVE_TELEMETRY_STORAGE_RESOURCE_ID
else
    unset AZCOPY_LIVE_TELEMETRY_STORAGE_PRINCIPAL AZCOPY_LIVE_TELEMETRY_STORAGE_RESOURCE_ID
fi
uname -a
df -h "$root"
"$root/bin/azcopy.test" -test.run='^TestTelemetryCLIPerformance$' -test.v -test.timeout="$((20 + 12 * AZCOPY_CLI_TELEMETRY_PERF_PAIRS))m" 2>&1 | tee "$AZCOPY_LIVE_TELEMETRY_OUTPUT/run.log"