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
  - local <-> Azure Files (Share/directory SAS authentication)
  - local <-> ADLS Gen 2 (SAS, OAuth, or SharedKey authentication)
  - Azure Blob (SAS or public) -> Azure Blob (SAS or OAuth authentication)
  - Azure Blob (SAS or public) -> Azure Files (SAS)
  - Azure Files (SAS) -> Azure Files (SAS)
  - Azure Files (SAS) -> Azure Blob (SAS or OAuth authentication)
  - AWS S3 (Access Key) -> Azure Block Blob (SAS or OAuth authentication)

Please refer to the examples for more information.

Advanced:
Please note that AzCopy automatically detects the Content Type of the files when uploading from the local disk, based on the file extension or content (if no extension is specified).

The built-in lookup table is small but on Unix it is augmented by the local system's mime.types file(s) if available under one or more of these names:
  - /etc/mime.types
  - /etc/apache2/mime.types
  - /etc/apache/mime.types

On Windows, MIME types are extracted from the registry. This feature can be turned off with the help of a flag. Please refer to the flag section.

` + environmentVariableNotice

const copyCmdExample = `Upload a single file using OAuth authentication. Please use 'azcopy login' command first if you aren't logged in yet:
- azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Same as above, but this time also compute MD5 hash of the file content and save it as the blob's Content-MD5 property. 
- azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --put-md5

Upload a single file with a SAS:
  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload a single file with a SAS using piping (block blobs only):
  - cat "/path/to/file.txt" | azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload an entire directory with a SAS:
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
or
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --put-md5

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

A note about wildcards in URLs:

The only usage of a wildcard in a URL that is supported is the final, trailing /*, and in the container name when not specifying a blob name.
If you happen to use the character * in the name of a blob, please manually encode it to %2A to avoid it being treated as a wildcard character.

Download the contents of a folder directly to the destination (rather than under a sub-directory):
 - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/folder]/*?[SAS]" "/path/to/dir"

Download an entire storage account at once
 - azcopy cp "https://[srcaccount].blob.core.windows.net/" "/path/to/dir" --recursive

Download an entire storage account at once with a wildcarded container name
 - azcopy cp "https://[srcaccount].blob.core.windows.net/[container*name]" "/path/to/dir" --recursive

Copy a single blob with SAS to another blob with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy a single blob with SAS to another blob with OAuth token. Please use 'azcopy login' command first if you aren't logged in yet. Note that the OAuth token is used to access the destination storage account:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]"

Copy an entire directory from blob virtual directory with SAS to another blob virtual directory with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Copy an entire account data from blob account with SAS to another blob account with SAS:
  - azcopy cp "https://[srcaccount].blob.core.windows.net?[SAS]" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy a single object from S3 with access key to blob with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3.amazonaws.com/[bucket]/[object]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy an entire directory from S3 with access key to blob virtual directory with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3.amazonaws.com/[bucket]/[folder]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
  - Please refer to https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html for what [folder] means for S3.

Copy all buckets in S3 service with access key to blob account with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3.amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy all buckets in a S3 region with access key to blob account with SAS:
  - Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
  - azcopy cp "https://s3-[region].amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true
`

// ===================================== ENV COMMAND ===================================== //
const envCmdShortDescription = "Shows the environment variables that can configure AzCopy's behavior"

const envCmdLongDescription = `Shows the environment variables that can configure AzCopy's behavior.

` + environmentVariableNotice

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
(In the progress information shown by this command, the byte counts and percent complete do not include files currently in progress.)
If the with-status flag is set, then the list of transfers in the job with the given value will be shown.`

const resumeJobsCmdShortDescription = "Resume the existing job with the given job ID"

const resumeJobsCmdLongDescription = `
Resume the existing job with the given job ID.`

const removeJobsCmdShortDescription = "Remove all files associated with the given job ID"

const removeJobsCmdLongDescription = `
Remove all files associated with the given job ID.

Note that the location of log files and job plan files can be customized. Please refer to the env command.`

const removeJobsCmdExample = "  azcopy jobs rm e52247de-0323-b14d-4cc8-76e0be2e2d44"

const cleanJobsCmdShortDescription = "Clean up log files and job plan files (used for progress tracking and resuming) for jobs"

const cleanJobsCmdLongDescription = `
Clean up log files and job plan files (used for progress tracking and resuming) for jobs.

Note that the location of log files and job plan files can be customized. Please refer to the env command.`

const cleanJobsCmdExample = "  azcopy jobs clean --with-status=completed"

// ===================================== LIST COMMAND ===================================== //
const listCmdShortDescription = "List the entities in a given resource"

const listCmdLongDescription = `List the entities in a given resource. Only Blob containers are supported at the moment.`

const listCmdExample = "azcopy list [containerURL]"

// ===================================== LOGIN COMMAND ===================================== //
const loginCmdShortDescription = "Log in to Azure Active Directory to access Azure Storage resources."

const loginCmdLongDescription = `Log in to Azure Active Directory to access Azure Storage resources. 
Note that, to be authorized to your Azure Storage account, you must assign your user 'Storage Blob Data Contributor' role on the Storage account.
This command will cache encrypted login information for current user using the OS built-in mechanisms.
Please refer to the examples for more information.

` + environmentVariableNotice

const environmentVariableNotice = "(NOTICE FOR SETTING ENVIRONMENT VARIABLES: Bear in mind that setting an environment variable from the command line " +
	"will be readable in your command line history. " +
	"For variables that contain credentials, consider clearing these entries from your history " +
	"or using a small script of sorts to prompt for and set these variables.)"

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

Log in as a service principal using a client secret:
- Set the environment variable AZCOPY_SPA_CLIENT_SECRET to the client secret for secret based service principal auth
- azcopy login --service-principal

Log in as a service principal using a certificate & password:
- Set the environment variable AZCOPY_SPA_CERT_PASSWORD to the certificate's password for cert based service principal auth
- azcopy login --service-principal --certificate-path /path/to/my/cert
Please treat /path/to/my/cert as a path to a PEM or PKCS12 file-- AzCopy does not reach into the system cert store to obtain your certificate.
--certificate-path is mandatory when doing cert-based service principal auth.
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
const removeCmdShortDescription = "Delete entities from Azure Storage Blob/File/ADLS Gen2"

const removeCmdLongDescription = `Delete entities from Azure Storage Blob/File/ADLS Gen2.`

const removeCmdExample = `
Remove a single blob with SAS:
  - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Remove an entire virtual directory with a SAS:
  - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Remove only the top blobs inside a virtual directory but not its sub-directories:
  - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false

Remove a subset of blobs in a virtual directory (ex: only jpg and pdf files, or if the blob name is "exactName"):
  - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --include="*.jpg;*.pdf;exactName"

Remove an entire virtual directory but exclude certain blobs from the scope (ex: every blob that starts with foo or ends with bar):
  - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --exclude="foo*;*bar"

Remove specific blobs and virtual directories by putting their relative paths (NOT URL-encoded) in a file:
  - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/parent/dir]" --recursive=true --list-of-files=/usr/bar/list.txt
  - file content:
	dir1/dir2
	blob1
	blob2

Remove a single file from ADLS Gen2 (include/exclude not supported):
  - azcopy rm "https://[account].dfs.core.windows.net/[container]/[path/to/file]?[SAS]"

Remove a single directory from ADLS Gen2 (include/exclude not supported):
  - azcopy rm "https://[account].dfs.core.windows.net/[container]/[path/to/directory]?[SAS]"
`

// ===================================== SYNC COMMAND ===================================== //
const syncCmdShortDescription = "Replicate source to the destination location"

const syncCmdLongDescription = `
Replicate a source to a destination location. The last modified times are used for comparison, the file is skipped if the last modified time in the destination is more recent. The supported pairs are:
  - local <-> Azure Blob (either SAS or OAuth authentication can be used)
  - Azure Blob <-> Azure Blob (Source must include a SAS or is publicly accessible; either SAS or OAuth authentication can be used for destination)
  - Azure File <-> Azure File (Source must include a SAS or is publicly accessible; SAS authentication should be used for destination)

Please note that the sync command differs from the copy command in several ways:
  0. The recursive flag is on by default.
  1. The source and destination should not contain patterns(such as * or ?).
  2. The include/exclude flags can be a list of patterns matching to the file names. Please refer to the example section for illustration.
  3. If there are files/blobs at the destination that are not present at the source, the user will be prompted to delete them. This prompt can be silenced by using the corresponding flags to automatically answer the deletion question.
  4. When syncing between virtual directories, add a trailing slash to the path (refer to examples) if there's a blob with the same name as one of the virtual directories 

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

Same as above, but this time also compute MD5 hash of the file content and save it as the blob's Content-MD5 property. 
  - azcopy sync "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --put-md5

Sync an entire directory including its sub-directories (note that recursive is by default on):
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]"
or
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --put-md5

Sync only the top files inside a directory but not its sub-directories:
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false

Sync a subset of files in a directory (ex: only jpg and pdf files, or if the file name is "exactName"):
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --include="*.jpg;*.pdf;exactName"

Sync an entire directory but exclude certain files from the scope (ex: every file that starts with foo or ends with bar):
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --exclude="foo*;*bar"

Sync a single blob:
  - azcopy sync "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Sync a virtual directory:
  - azcopy sync "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]?[SAS]" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=true

Sync a virtual directory sharing the same name as a blob (add a trailing slash to the path in order to disambiguate):
  - azcopy sync "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]/?[SAS]" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]/" --recursive=true

Sync an Azure File directory (same syntax as Blob):
  - azcopy sync "https://[account].file.core.windows.net/[share]/[path/to/dir]?[SAS]" "https://[account].file.core.windows.net/[share]/[path/to/dir]" --recursive=true

Note: if include/exclude flags are used together, only files matching the include patterns would be looked at, but those matching the exclude patterns would be always be ignored.
`

// ===================================== DOC COMMAND ===================================== //

const docCmdShortDescription = "Generates documentation for the tool in Markdown format"

const docCmdLongDescription = `Generates documentation for the tool in Markdown format, and stores them in the designated location.

By default, the files are stored in a folder named 'doc' inside the current directory.
`

// ===================================== BENCH COMMAND ===================================== //
const benchCmdShortDescription = "Performs a performance benchmark"

// TODO: document whether we delete the uploaded data

const benchCmdLongDescription = `
Runs a performance benchmark, by uploading uploading test data to a specified destination.
The test data is automatically generated.

The benchmark command runs the same upload process as 'copy', except that: 
  - there's no source parameter.  The command requires only a destination (which must be a blob container, 
    in the initial release of the benchmark command)
  - the payload is described by command line parameters, which control how many files are auto-generated and 
    how big they are. The generation process takes place entirely in memory. Disk is not used.
  - only a few of 'copy's optional parameters are supported
  - additional diagnostics are measured and reported
  - by default, the transferred data is deleted at the end of the test run

Benchmark mode will automatically tune itself to the number of parallel TCP connections that gives 
the maximum throughput. It will display that number at the end. To prevent auto-tuning, set the 
AZCOPY_CONCURRENCY_VALUE environment variable to a specific number of connections. 

All the usual authentication types are supported, however the most convenient approach for benchmarking is typically
to create an empty container with a SAS token and use SAS authentication.
`

const benchCmdExample = `Run a benchmark with default parameters (suitable for benchmarking networks up to 1 Gbps):'
- azcopy bench "https://[account].blob.core.windows.net/[container]?<SAS>"

Run a benchmark uploading 100 files, each 2 GiB in size: (suitable for benchmarking on a fast network, e.g. 10 Gbps):'
- azcopy bench "https://[account].blob.core.windows.net/[container]?<SAS>" --file-count 100 --size-per-file 2G

Same as above, but this time use 50,000 files, each 8 MiB in size and compute their MD5 hashes (the same way that --put-md5 does
in the copy command). The purpose of --put-md5 when benchmarking is to test whether MD5 computation affects throughput for the 
selected file count and size:
- azcopy bench "https://[account].blob.core.windows.net/[container]?<SAS>" --file-count 50000 --size-per-file 8M --put-md5
`
