# Azure Deployment Plan

Status: Validated

## 1. Scope

- Mode: modify an existing project.
- Provision durable Azure resources for AzCopy telemetry E2E validation.
- Replace the E2E pipeline's dependency on the manually managed
  `sharankur_insights1` Application Insights component.
- Preserve existing runtime behavior: E2E jobs continue discovering the
  Application Insights connection string and linked workspace from ARM.
- Fix Python E2E test-result propagation and add non-mutating PR validation for
  the telemetry Bicep.

## 2. Azure Context

- Subscription: `XDatamove-Dev-Playground1`
  (`31347be8-d066-464e-9866-7e58d85027b7`).
- Tenant: `72f988bf-86f1-41af-91ab-2d7cd011db47`.
- Location: `eastus`.
- Resource group: `azcopy-telemetry-test-rg`.
- The user confirmed the subscription and location.
- `Microsoft.OperationalInsights`, `Microsoft.Insights`, and
  `Microsoft.Authorization` are registered.
- Subscription policies were reviewed. The public-network Deny policy applies
  only to Key Vault, so it does not conflict with public telemetry ingestion
  and query. The enforced `Mover=Dev` Modify policy is satisfied explicitly by
  the Bicep resource tags.
- Microsoft.Quota exposes no adjustable regional quota rows for Application
  Insights or Log Analytics. Existing East US inventory is 11 workspaces and
  3 Application Insights components; this deployment adds one of each.

## 3. Architecture

- `infra/telemetry/main.bicep` is the single resource-group-scoped deployment
  definition.
- Deploy one `PerGB2018` Log Analytics workspace:
  `azcopy-telemetry-test-law`.
- Deploy one workspace-based Application Insights component:
  `azcopy-telemetry-test-ai`.
- Apply a 90-day retention period and 0.1 GB/day ingestion cap for synthetic E2E
  telemetry.
- Apply `service`, `environment`, `managedBy`, `Mover=Dev`, and `team` tags.
- Grant the runtime workload identity
  `28a5f214-22ad-42fc-833f-019f71f9bf60`:
  - Reader on the Application Insights component.
  - Log Analytics Reader on the workspace.
- Keep the connection string out of pipeline variables and secrets. The E2E
  job reads it from ARM at runtime.
- Keep infrastructure deployment as a separately selected mode of the existing
  AzCopy pipeline, not as part of normal build and E2E execution.

## 4. Pipeline Changes

- Add a PR/main job to compile `main.bicep`, `test.bicepparam`, and
  `prod.bicepparam` without mutating Azure.
- Preserve the Python E2E command's exit code while still producing coverage
  artifacts. Coverage conversion failures must also remain visible when tests
  pass.
- Add safe, compile-time-selected modes to the existing pipeline:
  `Tests` (default), `ValidateTelemetryInfrastructure`, and
  `DeployTelemetryInfrastructure`.
- Automatic CI and PR runs use `Tests`. Infrastructure modes omit all test jobs
  and include only the reusable telemetry stages, so normal runs never reference
  the deployment service connection.
- The reusable infrastructure stages perform compile, ARM validation, what-if,
  explicit manual approval for deployment, deployment, and a Log Analytics
  query smoke test.
- Keep `telemetry-test-infrastructure-pipeline.yml` as a no-trigger compatibility
  wrapper over the same reusable stages; it contains no duplicated deployment
  logic.
- Each E2E matrix leg runs an ingestion canary after building AzCopy and before
  starting the long test suite. The canary invokes `azcopy jobs list` with a
  unique correlation ID and polls for its `azcopy.command.invoked` event, so
  resource, RBAC, emission, and ingestion failures surface before the tests.
- Use a dedicated Azure DevOps workload-identity service connection named
  `azcopytelemetrydeploymentidentity`.
- Scope the deployment identity to the telemetry test resource group with:
  - Contributor.
  - Role Based Access Control Administrator (or User Access Administrator).
- Do not grant deployment rights to the runtime E2E identity.

## 5. Deployment

1. Create `azcopy-telemetry-test-rg` in East US.
2. Validate and preview:
   ```powershell
   az bicep build --file infra\telemetry\main.bicep
   az bicep build-params --file infra\telemetry\test.bicepparam
   az deployment group validate `
     --resource-group azcopy-telemetry-test-rg `
     --template-file infra\telemetry\main.bicep `
     --parameters infra\telemetry\test.bicepparam
   az deployment group what-if `
     --resource-group azcopy-telemetry-test-rg `
     --template-file infra\telemetry\main.bicep `
     --parameters infra\telemetry\test.bicepparam
   ```
3. Deploy only after validation succeeds:
   ```powershell
   az deployment group create `
     --resource-group azcopy-telemetry-test-rg `
     --name azcopy-telemetry-test `
     --template-file infra\telemetry\main.bicep `
     --parameters infra\telemetry\test.bicepparam
   ```
4. Update Azure Pipeline variables:
   - `AZCOPY_TELEMETRY_SUBSCRIPTION_ID_TEST` =
     `31347be8-d066-464e-9866-7e58d85027b7`
   - `AZCOPY_TELEMETRY_RESOURCE_GROUP_TEST` =
     `azcopy-telemetry-test-rg`
   - `AZCOPY_TELEMETRY_APP_NAME_TEST` =
     `azcopy-telemetry-test-ai`

## 6. Verification

- Confirm both resources are in `Succeeded` provisioning state.
- Confirm both runtime RBAC assignments exist at resource scope.
- Query the workspace with:
  ```kusto
  AppEvents
  | take 1
  ```
  An empty result is acceptable; authorization/query failure is not.
- Run targeted telemetry unit tests.
- Validate the changed Azure Pipelines YAML.
- Trigger the main E2E pipeline and verify ARM discovery, telemetry ingestion,
  and terminal-event validation.
- Terminal-event validation tracks only commands that use job-attempt telemetry
  (`copy`, `sync`, and `jobs resume`). Commands such as `remove` emit
  `azcopy.command.invoked` and must not be treated as missing
  `azcopy.job.finished` events.
- If legacy S2S tests fail with SAS signature errors, regenerate
  `S2S_SRC_BLOB_ACCOUNT_SAS_URL` and `S2S_SRC_FILE_ACCOUNT_SAS_URL`.

## 7. Validation Proof

Validated at `2026-08-18T10:22:31Z` against subscription
`31347be8-d066-464e-9866-7e58d85027b7` and resource group
`azcopy-telemetry-test-rg`.

| Check | Command | Result |
|---|---|---|
| Bicep lint | `az bicep lint --file .\infra\telemetry\main.bicep` | Passed with no diagnostics. |
| Main template compile | `az bicep build --file .\infra\telemetry\main.bicep --stdout` | Passed. |
| Test parameters compile | `az bicep build-params --file .\infra\telemetry\test.bicepparam --stdout` | Passed. |
| Production parameters compile | `az bicep build-params --file .\infra\telemetry\prod.bicepparam --stdout` | Passed. |
| ARM validation | `az deployment group validate --subscription 31347be8-d066-464e-9866-7e58d85027b7 --resource-group azcopy-telemetry-test-rg --name azcopy-telemetry-test-preflight --template-file .\infra\telemetry\main.bicep --parameters .\infra\telemetry\test.bicepparam` | `Succeeded`; no ARM error. |
| ARM what-if | `az deployment group what-if --subscription 31347be8-d066-464e-9866-7e58d85027b7 --resource-group azcopy-telemetry-test-rg --name azcopy-telemetry-test-whatif --template-file .\infra\telemetry\main.bicep --parameters .\infra\telemetry\test.bicepparam` | `Succeeded`; exactly two resources and two scoped role assignments will be created; no deletes or unsupported changes. |
| Policy review | `az policy assignment list` plus policy-definition inspection through `az rest` | The enforced public-network Deny policy targets only `Microsoft.KeyVault/vaults`; the planned telemetry resources are not affected. The enforced `Mover=Dev` Modify policy is satisfied explicitly in Bicep. |
| Static RBAC review | Review of `infra/telemetry/main.bicep` and what-if role payloads | Runtime identity receives Reader only on Application Insights and Log Analytics Reader only on the workspace. Role-definition IDs and scopes are correct. |
| Source diff validation | `git diff --check` | Passed. |
| Deployment | `az deployment group create --subscription 31347be8-d066-464e-9866-7e58d85027b7 --resource-group azcopy-telemetry-test-rg --name azcopy-telemetry-test --template-file .\infra\telemetry\main.bicep --parameters .\infra\telemetry\test.bicepparam` | `Succeeded` at `2026-08-18T10:25:02Z` in 40.47 seconds. |
| Resource verification | `az resource show` and `az monitor log-analytics workspace show` | `azcopy-telemetry-test-ai` and `azcopy-telemetry-test-law` both report `Succeeded`. |
| Live RBAC verification | `az role assignment list` at each resource scope for principal `28a5f214-22ad-42fc-833f-019f71f9bf60` | Reader exists only on Application Insights; Log Analytics Reader exists only on the workspace. |
| Query authorization smoke test | `az monitor log-analytics query --workspace 8e1cadf5-6061-41cf-a671-1b369510931c --analytics-query "AppEvents \| take 1"` | Authorized successfully; an empty result is expected before the next E2E run. |
| E2E pipeline variable cutover | `az pipelines variable update` and `az pipelines variable list --pipeline-id 2` | Pipeline `Azure.azure-storage-azcopy` now targets subscription `31347be8-d066-464e-9866-7e58d85027b7`, resource group `azcopy-telemetry-test-rg`, and Application Insights component `azcopy-telemetry-test-ai`. |

The application build is not part of this infrastructure-only deployment. The
changed infrastructure and both parameter files compiled successfully. A
standalone Python YAML parse was unavailable on this machine; Azure Pipelines
will perform authoritative parsing when the pipeline definition is created or
queued.

The dedicated workload-identity Azure Resource Manager service connection
`azcopytelemetrydeploymentidentity` has been created in project
`AzCopy-NextGen`. Existing pipeline ID 2 must be authorized to use it before a
manual infrastructure mode can execute. Its principal has Contributor and Role
Based Access Control Administrator scoped only to
`azcopy-telemetry-test-rg`.
