# E2E Application Insights Telemetry Validation Plan

## Goal

Validate, as part of the existing AzCopy end-to-end test run, that job telemetry emitted by the test AzCopy process reaches a dedicated Application Insights resource. The validation must use a runtime connection-string override so the E2E binary is not linked to either the production or test telemetry resource.

EV2 is intentionally not part of this design. The persistent test resource is provisioned with Bicep independently of the E2E pipeline.

## Architecture

1. A persistent test resource group contains:
   - A Log Analytics workspace.
   - A workspace-based Application Insights component.
   - Optional least-privilege role assignments for the E2E workload identity.
2. The E2E pipeline authenticates with workload identity and reads the Application Insights connection string and Log Analytics workspace customer ID from ARM at runtime.
3. Each E2E matrix job generates a unique telemetry run ID.
4. The E2E framework passes these values to every AzCopy child process:
   - `AZCOPY_TELEMETRY_CONNECTION_STRING`
   - `AZCOPY_E2E_TELEMETRY_RUN_ID`
5. E2E AzCopy invocations use a telemetry sampling rate of `1`, ensuring deterministic emission.
6. AzCopy adds the E2E run ID as an optional telemetry property. It is absent from normal customer telemetry.
7. The E2E framework records each observed job ID and, after the suites finish, polls the Log Analytics query endpoint until the corresponding `azcopy.job.finished` events arrive or a bounded timeout expires.

## Configuration Contract

| Setting | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `AZCOPY_TELEMETRY_CONNECTION_STRING` | E2E pipeline | AzCopy child process | Runtime-only ingestion destination override |
| `AZCOPY_E2E_TELEMETRY_RUN_ID` | E2E pipeline | AzCopy and E2E verifier | Isolates one matrix leg from concurrent runs |
| `NEW_E2E_APP_INSIGHTS_WORKSPACE_ID` | E2E pipeline | E2E verifier | Log Analytics workspace customer ID used by the query API |
| `AZCOPY_TELEMETRY_SUBSCRIPTION_ID_TEST` | Pipeline variable | E2E pipeline | Dedicated telemetry subscription |
| `AZCOPY_TELEMETRY_RESOURCE_GROUP_TEST` | Pipeline variable | E2E pipeline | Resource group containing test telemetry resources |
| `AZCOPY_TELEMETRY_APP_NAME_TEST` | Pipeline variable | E2E pipeline | Application Insights component name |

Validation is disabled for local runs unless both the run ID and workspace ID are configured. A partially configured run fails during framework setup with a clear error.

## E2E Verification Behavior

- Validation starts after E2E configuration and workload-identity setup.
- The suite records the UTC start time before AzCopy jobs are launched.
- After each AzCopy process exits, the framework records the parsed job ID when one is available.
- Repeated attempts for the same job ID, including resume operations, increment the expected event count.
- Suite teardown queries only:
  - Events at or after the suite start time.
  - `azcopy.job.finished`.
  - Events whose `E2ETestRunID` matches the current matrix job.
- Polling defaults:
  - Interval: 15 seconds.
  - Timeout: 5 minutes.
- Authentication uses the existing `DefaultAzureCredential`/workload-identity path and requests the `https://api.loganalytics.io/.default` scope.
- HTTP 408, 429, and transient 5xx responses are retried within the deadline. Authentication, authorization, malformed-query, and malformed-response failures stop immediately.
- Timeout diagnostics list each missing job ID, expected count, received count, the run ID, and the KQL query.

The first implementation asserts terminal job events because they are emitted synchronously at the end of an AzCopy job attempt. Start-event delivery remains best-effort and is not used as a suite pass/fail signal.

## Infrastructure

`infra/telemetry/main.bicep` remains the single deployment definition. It provisions the workspace and Application Insights component and may assign:

- Log Analytics Reader on the workspace.
- Reader on the Application Insights component.

Role assignments are conditional on an E2E query principal object ID, allowing environments that manage RBAC separately to omit them.

The infrastructure pipeline performs:

1. Bicep compilation.
2. ARM deployment validation.
3. ARM what-if.
4. Idempotent resource-group deployment.
5. A smoke query against the deployed workspace.

The connection string is not stored as an output variable or secret. The E2E pipeline retrieves it directly from ARM for each run and keeps it process-scoped.

## E2E Pipeline Changes

- Continue embedding the production connection string only in production builds.
- Remove test connection-string `-ldflags` from E2E builds.
- Retrieve the dedicated test component and workspace details in the Azure CLI task.
- Generate a run ID from stable pipeline identifiers plus the matrix leg name.
- Export the connection string, workspace customer ID, and run ID only to the E2E test process.
- Keep the runtime connection-string validation so accidentally using the production resource fails before tests start.

## Test Strategy

### Unit Tests

- Optional E2E run ID is omitted from normal telemetry.
- Configured E2E run ID is serialized on job and command events and is bounded by the telemetry property limit.
- Validation configuration rejects partial setup.
- Expected event collection is concurrency safe and counts repeated job IDs.
- Log Analytics responses are parsed by column name rather than fixed column order.
- Polling succeeds after eventual consistency, retries transient failures, and reports missing events on timeout.

### Focused Validation

- Run telemetry package tests.
- Run AzCopy telemetry tests.
- Run E2E package tests that cover the verifier and command construction.
- Compile Bicep and validate pipeline YAML through the repository's existing checks where available.
- In the dedicated subscription, deploy the Bicep infrastructure and run one E2E matrix leg before enabling all legs.

## Rollout

1. Merge the code, Bicep, and pipeline changes with E2E validation disabled by default.
2. Deploy the persistent test telemetry resources.
3. Grant the workload identity query access.
4. Configure the three test resource locator pipeline variables.
5. Run one E2E matrix leg and confirm telemetry ingestion and query permissions.
6. Enable all E2E matrix legs.
7. Monitor ingestion volume and the workspace daily cap during the first several full runs.

## Required Deployment Inputs

- Dedicated telemetry subscription ID.
- Telemetry resource-group name.
- Application Insights component name.
- Object ID of the E2E workload identity if Bicep should manage query RBAC.
- Confirmation that the E2E Azure service connection can read the component and obtain a Log Analytics query token in the dedicated subscription.
