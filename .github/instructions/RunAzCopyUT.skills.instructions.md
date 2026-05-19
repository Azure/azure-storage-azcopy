---
description: Build AzCopy and run unit tests locally on WSL
applyTo: '**/*.go'
---

# Skill: Build AzCopy & Run Unit Tests Locally

## Overview
Build the AzCopy project for Linux and run the unit test suite locally using WSL.

## Prerequisites (once per WSL session)

Before running tests, set the required environment variables in the WSL session:

```bash
export GOOS=linux
export ACCOUNT_NAME="azcopyutsa1"
export AZCOPY_E2E_ACCOUNT_NAME="azcopyutsa1"
```

Then prompt the user for the storage account access key and set:

```bash
export ACCOUNT_KEY=<AccessKey>
export AZCOPY_E2E_ACCOUNT_KEY=<AccessKey>
```

> **Note:** Ask the user to provide the `AccessKey` value. Do not hardcode or guess it.

## Step 1: Build

Cross-compile for Linux (required since the project uses Linux-specific syscalls):

```bash
GOOS=linux go build ./...
```

Ensure the build succeeds with exit code 0 before proceeding to tests.

## Step 2: Run Unit Tests

```bash
go test -count=1 -timeout=45m -v ./cmd ./common ./sddl ./azcopy ./mock_server
```

### Flags
| Flag | Purpose |
|------|---------|
| `-count=1` | Disable test caching so tests always re-run |
| `-timeout=45m` | Allow up to 45 minutes for the full suite |
| `-v` | Verbose output for each test |

### Test packages
- `./cmd` — CLI command tests
- `./common` — Shared utilities tests
- `./sddl` — SDDL parser tests
- `./azcopy` — Core client tests
- `./mock_server` — Mock server tests

## Completion Checks
- Build exits with code 0
- All tests pass (look for `PASS` in output, no `FAIL`)
- If tests fail, review the failing test name and error message before retrying