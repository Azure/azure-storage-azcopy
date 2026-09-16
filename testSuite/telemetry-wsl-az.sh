#!/usr/bin/env bash
set -euo pipefail
: "${WSL_AZURE_CONFIG_DIR:?Set the existing Windows Azure CLI config directory as a WSL path}"
export AZURE_CONFIG_DIR
AZURE_CONFIG_DIR="$(wslpath -w "$WSL_AZURE_CONFIG_DIR")"
exec '/mnt/c/Program Files (x86)/Microsoft SDKs/Azure/CLI2/python.exe' -IBm azure.cli "$@"