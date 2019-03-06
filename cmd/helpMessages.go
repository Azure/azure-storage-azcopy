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
Copies source data to a destination location. The supported directions are:
  - local <-> Azure Blob (SAS or OAuth authentication)
  - local <-> Azure File (SAS authentication)
  - local <-> ADLS Gen 2 (OAuth or SharedKey authentication)
  - Azure Block Blob (SAS or public) <-> Azure Block Blob (SAS or OAuth authentication)
  - Azure File (SAS) -> Azure Block Blob (SAS or OAuth authentication)
  - AWS S3 (Access Key) -> Azure Block Blob (SAS or OAuth authentication)

Please refer to the examples for more information.

Advanced:
Please note that AzCopy automatically detects the Content Type of the files when uploading from the local disk, based on the file extension or content (if no extension is specified).

The built-in lookup table is small but on Unix it is augmented by the local system's mime.types file(s) if available under one or more of these names:
  - /etc/mime.types
  - /etc/apache2/mime.types
  - /etc/apache/mime.types

On Windows, MIME types are extracted from the registry. This feature can be turned off with the help of a flag. Please refer to the flag section.
`

const copyCmdExample = `Upload a single file using OAuth authentication. Please use 'azcopy login' command first if you aren't logged in yet:
- azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Upload a single file with a SAS:
  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload a single file with a SAS using piping (block blobs only):
  - cat "/path/to/file.txt" | azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload an entire directory with a SAS:
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Upload a set of files with a SAS using wildcards:
  - azcopy cp "/path/*foo/*bar/*.pdf" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]"

Upload files and directories with a SAS using wildcards:
  - azcopy cp "/path/*foo/*bar*" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Download a single file using OAuth authentication. Please use 'azcopy login' command first if you aren't logged in yet:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]" "/path/to/file.txt"

Download a single file with a SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "/path/to/file.txt"

Download a single file with a SAS using piping (block blobs only):
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" > "/path/to/file.txt"

Download an entire directory with a SAS:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "/path/to/dir" --recursive=true

Download a set of files with a SAS using wildcards:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/foo*?[SAS]" "/path/to/dir"

Download files and directories with a SAS using wildcards:
  - azcopy cp "https://[account].blob.core.windows.net/[container]/foo*?[SAS]" "/path/to/dir" --recursive=true

Copy a single blob with SAS to blob with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy a single blob with SAS to blob with OAuth token. Please use 'azcopy login' command first if you aren't logged in yet. Note that the OAuth token is used to access the destination storage account:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]"

Copy an entire directory from blob virtual directory with SAS to blob virtual directory with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Copy an entire account data from blob account with SAS to blob account with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net?[SAS]" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy a single object from S3 with access key to blob with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3.amazonaws.com/[bucket]/[object]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy an entire directory from S3 with access key to blob virtual directory with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3.amazonaws.com/[bucket]/[object-as-virtual-directory]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Copy all buckets in S3 service with access key to blob account with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3.amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true
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
Show detailed information for the given job ID: if only the job ID is supplied without a flag, then the progress summary of the job is returned.
If the with-status flag is set, then the list of transfers in the job with the given value will be shown.`

const resumeJobsCmdShortDescription = "Resume the existing job with the given job ID"

const resumeJobsCmdLongDescription = `
Resume the existing job with the given job ID.`

// ===================================== LIST COMMAND ===================================== //
const listCmdShortDescription = "List the entities in a given resource"

const listCmdLongDescription = `List the entities in a given resource. Only Blob containers are supported at the moment.`

const listCmdExample = "azcopy list [containerURL]"

// ===================================== LOGIN COMMAND ===================================== //
const loginCmdShortDescription = "Log in to Azure Active Directory to access Azure Storage resources."

const loginCmdLongDescription = `Log in to Azure Active Directory to access Azure Storage resources. 
Note that, to be authorized to your Azure Storage account, you must assign your user 'Storage Blob Data Contributor' role on the Storage account.
This command will cache encrypted login information for current user using the OS built-in mechanisms.
Please refer to the examples for more information.`

const loginCmdExample = `Log in interactively with default AAD tenant ID set to common:
- azcopy login

Log in interactively with a specified tenant ID:
- azcopy login --tenant-id "[TenantID]"

Log in using a VM's system-assigned identity:
- azcopy login --identity

Log in using a VM's user-assigned identity with a Client ID of the service identity:
- azcopy login --identity --identity-client-id "[ServiceIdentityClientID]"

Log in using a VM's user-assigned identity with an Object ID of the service identity:
- azcopy login --identity --identity-object-id "[ServiceIdentityObjectID]"

Log in using a VM's user-assigned identity with a Resource ID of the service identity:
- azcopy login --identity --identity-resource-id "/subscriptions/<subscriptionId>/resourcegroups/myRG/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myID"

`

// ===================================== LOGOUT COMMAND ===================================== //
const logoutCmdShortDescription = "Log out to terminate access to Azure Storage resources."

const logoutCmdLongDescription = `Log out to terminate access to Azure Storage resources.
This command will remove all the cached login information for the current user.`

// ===================================== MAKE COMMAND ===================================== //
const makeCmdShortDescription = "Create a container/share/filesystem"

const makeCmdLongDescription = `Create a container/share/filesystem represented by the given resource URL.`

const makeCmdExample = `
  - azcopy make "https://[account-name].[blob,file,dfs].core.windows.net/[top-level-resource-name]"
`

// ===================================== REMOVE COMMAND ===================================== //
const removeCmdShortDescription = "Delete blobs or files from Azure Storage"

const removeCmdLongDescription = `Delete blobs or files from Azure Storage.`

// ===================================== SYNC COMMAND ===================================== //
const syncCmdShortDescription = "Replicate source to the destination location"

const syncCmdLongDescription = `
Replicate a source to a destination location. The last modified times are used for comparison, the file is skipped if the last modified time in the destination is more recent. The supported pairs are:
  - local <-> Azure Blob (either SAS or OAuth authentication can be used)

Please note that the sync command differs from the copy command in several ways:
  0. The recursive flag is on by default.
  1. The source and destination should not contain patterns(such as * or ?).
  2. The include/exclude flags can be a list of patterns matching to the file names. Please refer to the example section for illustration.
  3. If there are files/blobs at the destination that are not present at the source, the user will be prompted to delete them. This prompt can be silenced by using the corresponding flags to automatically answer the deletion question.

Advanced:
Please note that AzCopy automatically detects the Content Type of the files when uploading from the local disk, based on the file extension or content (if no extension is specified).

The built-in lookup table is small but on Unix it is augmented by the local system's mime.types file(s) if available under one or more of these names:
  - /etc/mime.types
  - /etc/apache2/mime.types
  - /etc/apache/mime.types

On Windows, MIME types are extracted from the registry.
`

const syncCmdExample = `
Sync a single file:
  - azcopy sync "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Sync an entire directory including its sub-directories (note that recursive is by default on):
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]"

Sync only the top files inside a directory but not its sub-directories:
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false

Sync a subset of files in a directory (ex: only jpg and pdf files, or if the file name is "exactName"):
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --include="*.jpg;*.pdf;exactName"

Sync an entire directory but exclude certain files from the scope (ex: every file that starts with foo or ends with bar):
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --exclude="foo*;*bar"

Note: if include/exclude flags are used together, only files matching the include patterns would be looked at, but those matching the exclude patterns would be always be ignored.
`
