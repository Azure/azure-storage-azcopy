package cmd

import "github.com/Azure/azure-storage-azcopy/common"

// ===================================== ROOT COMMAND ===================================== //
const rootCmdShortDescription = "AzCopy is a command line tool that moves data into/out of Azure Storage."

const rootCmdLongDescription = "AzCopy " + common.AzcopyVersion +
	`
Project URL: github.com/Azure/azure-storage-azcopy

AzCopy is a command line tool that moves data into/out of Azure Storage.
To report issues or to learn more about the tool, go to github.com/Azure/azure-storage-azcopy

The general format of the commands is: 'azcopy [command] [arguments] --[flag-name]=[flag-value]'.
`

// ===================================== COPY COMMAND ===================================== //
const copyCmdShortDescription = "Copies source data to a destination location"

const copyCmdLongDescription = `
Copies source data to a destination location. The supported pairs are:
  - local <-> Azure Blob (SAS or OAuth authentication)
  - local <-> Azure File (SAS authentication)
  - local <-> ADLS Gen 2 (OAuth or SharedKey authentication)
  - Azure Block Blob (SAS or public) <-> Azure Block Blob (SAS or OAuth authentication)

Please refer to the examples for more information.

Advanced:
Please note that AzCopy automatically detects the Content-Type of files when uploading from local disk, based on file extension or file content(if no extension).

The built-in lookup table is small but on unix it is augmented by the local system's mime.types file(s) if available under one or more of these names:
  - /etc/mime.types
  - /etc/apache2/mime.types
  - /etc/apache/mime.types

On Windows, MIME types are extracted from the registry. This feature can be turned off with the help of a flag. Please refer to the flag section.
`

const copyCmdExample = `Upload a single file with SAS:
  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload a single file with OAuth token, please use login command first if not yet logged in:
  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Upload a single file through piping(block blob only) with SAS:
  - cat "/path/to/file.txt" | azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload an entire directory with SAS:
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Upload only files using wildcards with SAS:
  - azcopy cp "/path/*foo/*bar/*.pdf" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]"

Upload files and directories using wildcards with SAS:
  - azcopy cp "/path/*foo/*bar*" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Download a single file with SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "/path/to/file.txt"

Download a single file with OAuth token, please use login command first if not yet logged in:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]" "/path/to/file.txt"

Download a single file through piping(blobs only) with SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" > "/path/to/file.txt"

Download an entire directory with SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "/path/to/dir" --recursive=true

Download files using wildcards with SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/foo*?[SAS]" "/path/to/dir"

Download files and directories using wildcards with SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/foo*?[SAS]" "/path/to/dir" --recursive=true

Copy a single file with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy a single file with OAuth token, please use login command first if not yet logged in and note that OAuth token is used by destination:
- azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]"

Copy an entire directory with SAS:
- azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Copy an entire account with SAS:
- azcopy cp "https://[srcaccount].blob.core.windows.net?[SAS]" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

`

// ===================================== ENV COMMAND ===================================== //
const envCmdShortDescription = "Shows the environment variables that can configure AzCopy's behavior"

const envCmdLongDescription = `Shows the environment variables that can configure AzCopy's behavior.`

// ===================================== JOBS COMMAND ===================================== //
const jobsCmdShortDescription = "Sub-commands related to managing jobs"

const jobsCmdLongDescription = "Sub-commands related to managing jobs."

const jobsCmdExample = "azcopy jobs show [jobID]"

const listJobsCmdShortDescription = "Display information on all jobs"

const listJobsCmdLongDescription = `
Display information on all jobs.`

const showJobsCmdShortDescription = "Show detailed information for the given job ID"

const showJobsCmdLongDescription = `
Show detailed information for the given job ID: if only the job ID is supplied without flag, then the progress summary of the job is returned.
If the with-status flag is set, then the list of transfers in the job with the given value will be shown.`

const resumeJobsCmdShortDescription = "Resume the existing job with the given job ID"

const resumeJobsCmdLongDescription = `
Resume the existing job with the given job ID.`

// ===================================== LIST COMMAND ===================================== //
const listCmdShortDescription = "List the entities in a given resource"

const listCmdLongDescription = `List the entities in a given resource. Only Blob containers are supported at the moment.`

const listCmdExample = "azcopy list [containerURL]"

// ===================================== LOGIN COMMAND ===================================== //
const loginCmdShortDescription = "Log in to Azure Active Directory to access Azure storage resources."

const loginCmdLongDescription = `Log in to Azure Active Directory to access Azure storage resources. 
		Note that, to be authorized to your Azure Storage account, you must assign your user 'Storage Blob Data Contributor' role on the Storage account.
This command will cache encrypted login info for current user with OS built-in mechanisms.
Please refer to the examples for more information.`

const loginCmdExample = `Log in interactively with default AAD tenant ID set to common:
- azcopy login

Log in interactively with specified tenant ID:
- azcopy login --tenant-id "[TenantID]"

Log in using a VM's system-assigned identity:
- azcopy login --identity

Log in using a VM's user-assigned identity with Client ID of the service identity:
- azcopy login --identity --identity-client-id "[ServiceIdentityClientID]"

Log in using a VM's user-assigned identity with Object ID of the service identity:
- azcopy login --identity --identity-object-id "[ServiceIdentityObjectID]"

Log in using a VM's user-assigned identity with Resource ID of the service identity:
- azcopy login --identity --identity-resource-id "/subscriptions/<subscriptionId>/resourcegroups/myRG/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myID"

`

// ===================================== LOGOUT COMMAND ===================================== //
const logoutCmdShortDescription = "Log out to remove access to Azure storage resources."

const logoutCmdLongDescription = `Log out to remove access to Azure storage resources.
This command will remove all the cached login info for current user.`

// ===================================== MAKE COMMAND ===================================== //
const makeCmdShortDescription = "Create a container/share/filesystem"

const makeCmdLongDescription = `Create a container/share/filesystem represented by the given resource URL.`

const makeCmdExample = `
  - azcopy make "https://[account-name].[blob,file,dfs].core.windows.net/[top-level-resource-name]"
`

// ===================================== REMOVE COMMAND ===================================== //
const removeCmdShortDescription = "Deletes blobs or files in Azure Storage"

const removeCmdLongDescription = `Deletes blobs or files in Azure Storage.`

// ===================================== SYNC COMMAND ===================================== //
const syncCmdShortDescription = "Replicates source to the destination location"

const syncCmdLongDescription = `
Replicates source to the destination location. The last modified times are used for comparison. The supported pairs are:
  - local <-> Azure Blob (SAS or OAuth authentication)

Advanced:
Please note that AzCopy automatically detects the Content-Type of files when uploading from local disk, based on file extension or file content(if no extension).

The built-in lookup table is small but on unix it is augmented by the local system's mime.types file(s) if available under one or more of these names:
  - /etc/mime.types
  - /etc/apache2/mime.types
  - /etc/apache/mime.types

On Windows, MIME types are extracted from the registry.
`
