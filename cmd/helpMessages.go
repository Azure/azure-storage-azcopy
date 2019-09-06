package cmd

import "github.com/Azure/azure-storage-azcopy/common"

// ===================================== ROOT COMMAND ===================================== //
const rootCmdShortDescription = "AzCopy is a command-line tool that moves data into and out of Azure Storage."

const rootCmdLongDescription = "AzCopy " + common.AzcopyVersion +
	`
  The general format of the commands is: azcopy [command] [arguments] --[flag-name]=[flag-value].

  To report issues or to learn more about the tool, see [https://github.com/Azure/azure-storage-azcopy](https://github.com/Azure/azure-storage-azcopy).
`

// ===================================== COPY COMMAND ===================================== //
const copyCmdShortDescription = "Copies source data to a destination location"

const copyCmdLongDescription = `

The supported directions are:

- local <-> Azure Blob (SAS or OAuth authentication)
- local <-> Azure File (Share/directory SAS authentication)
- local <-> Azure Data Lake Storage Gen2 (SAS, OAuth, or SharedKey authentication)
- Azure Blob (SAS or public) <-> Azure Blob (SAS or OAuth authentication)
- Azure File (SAS) -> Azure Block Blob (SAS or OAuth authentication)
- AWS S3 (Access Key) -> Azure Block Blob (SAS or OAuth authentication)

Please refer to the examples for more information.

### Advanced

AzCopy automatically detects the content type of the files when uploading from the local disk, based on the file extension or content (if no extension is specified).

The built-in lookup table is small, but on Unix, it is augmented by the local system's mime.types file(s) if available under one or more of these names:

- /etc/mime.types
- /etc/apache2/mime.types
- /etc/apache/mime.types

On Windows, MIME types are extracted from the registry. This feature can be turned off with the help of a flag. Please refer to the flag section.

` + environmentVariableNotice

const copyCmdExample = `Upload a single file using OAuth authentication.

If you have not yet logged into AzCopy, please use azcopy login command before you run the following command.
` + exampleSnippetStart + `
azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" 
` + exampleSnippetEnd + `


Same as above, but this time also compute MD5 hash of the file content and save it as the blob's Content-MD5 property:

` + exampleSnippetStart + `
azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --put-md5
` + exampleSnippetEnd + `

Upload a single file with a SAS:

` + exampleSnippetStart + `
azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"
` + exampleSnippetEnd + `

Upload a single file with a SAS using piping (block blobs only):

` + exampleSnippetStart + `
cat "/path/to/file.txt" | azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"
` + exampleSnippetEnd + `

Upload an entire directory with a SAS:

` + exampleSnippetStart + `
azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
` + exampleSnippetEnd + `

or

` + exampleSnippetStart + `
azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --put-md5
` + exampleSnippetEnd + `

Upload a set of files with a SAS using wildcards:

` + exampleSnippetStart + `
azcopy cp "/path/*foo/*bar/*.pdf" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]"
` + exampleSnippetEnd + `

Upload files and directories with a SAS using wildcards:

` + exampleSnippetStart + `
azcopy cp "/path/*foo/*bar*" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
` + exampleSnippetEnd + `

Download a single file using OAuth authentication.

If you have not yet logged into AzCopy, please use azcopy login command before you run the following command.

` + exampleSnippetStart + `
azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]" "/path/to/file.txt"
` + exampleSnippetEnd + `

Download a single file with a SAS:

` + exampleSnippetStart + `
azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "/path/to/file.txt"
` + exampleSnippetEnd + `

Download a single file with a SAS using piping (block blobs only):

` + exampleSnippetStart + `
azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" > "/path/to/file.txt"
` + exampleSnippetEnd + `

Download an entire directory with a SAS:

` + exampleSnippetStart + `
azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "/path/to/dir" --recursive=true
` + exampleSnippetEnd + `

Download a set of files with a SAS using wildcards:

` + exampleSnippetStart + `
azcopy cp "https://[account].blob.core.windows.net/[container]/foo*?[SAS]" "/path/to/dir"
` + exampleSnippetEnd + `

Download files and directories with a SAS using wildcards:

` + exampleSnippetStart + `
azcopy cp "https://[account].blob.core.windows.net/[container]/foo*?[SAS]" "/path/to/dir" --recursive=true
` + exampleSnippetEnd + `

Copy a single blob with SAS to another blob with SAS:

` + exampleSnippetStart + `
azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"
` + exampleSnippetEnd + `

Copy a single blob with SAS to another blob with OAuth token.

If you have not yet logged into AzCopy, please use azcopy login command before you run the following command. The OAuth token is used to access the destination storage account.

` + exampleSnippetStart + `
azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]"
` + exampleSnippetEnd + `

Copy an entire directory from blob virtual directory with SAS to another blob virtual directory with SAS:

` + exampleSnippetStart + `
azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
` + exampleSnippetEnd + `

Copy an entire account data from blob account with SAS to another blob account with SAS:

` + exampleSnippetStart + `
azcopy cp "https://[srcaccount].blob.core.windows.net?[SAS]" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true
` + exampleSnippetEnd + `

Copy a single object from S3 with access key to blob with SAS:

Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.

` + exampleSnippetStart + `
azcopy cp "https://s3.amazonaws.com/[bucket]/[object]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"
` + exampleSnippetEnd + `

Copy an entire directory from S3 with access key to blob virtual directory with SAS:

` + exampleSnippetStart + `
azcopy cp "https://s3.amazonaws.com/[bucket]/[folder]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
` + exampleSnippetEnd + `

See https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html to learn about what [folder] means for S3. Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.

Copy all buckets in S3 service with access key to blob account with SAS:

Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.

` + exampleSnippetStart + `
azcopy cp "https://s3.amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true
` + exampleSnippetEnd + `

Copy all buckets in a S3 region with access key to blob account with SAS:

` + exampleSnippetStart + `
azcopy cp "https://s3-[region].amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true
` + exampleSnippetEnd + `

Set environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for S3 source.
`

// ===================================== ENV COMMAND ===================================== //
const envCmdShortDescription = "Shows the environment variables that can configure AzCopy's behavior"

const envCmdLongDescription = `` + environmentVariableNotice

// ===================================== JOBS COMMAND ===================================== //
const jobsCmdShortDescription = "Sub-commands related to managing jobs"

const jobsCmdLongDescription = ""

const jobsCmdExample = `
` + exampleSnippetStart + `
azcopy jobs show [jobID]
` + exampleSnippetEnd + `
`

const listJobsCmdShortDescription = "Displays information on all jobs"

const listJobsCmdLongDescription = ``

const showJobsCmdShortDescription = "Shows detailed information for the given job ID"

const showJobsCmdLongDescription = `
If only the job ID is supplied without a flag, then the progress summary of the job is returned.

If the with-status flag is set, then the list of transfers in the job with the given value will be shown.

`

const resumeJobsCmdShortDescription = "Resumes the existing job with the given job ID"

const resumeJobsCmdLongDescription = `
` + exampleSnippetStart + `
azcopy jobs resume [jobID] [flags]
` + exampleSnippetEnd + `
`

// ===================================== LIST COMMAND ===================================== //
const listCmdShortDescription = "Lists the entities in a given resource"

const listCmdLongDescription = `Only Blob containers are supported in the current release.`

const listCmdExample = "azcopy list [containerURL]"

// ===================================== LOGIN COMMAND ===================================== //
const loginCmdShortDescription = "Logs in to Azure Active Directory to access Azure Storage resources."

const loginCmdLongDescription = `Log in to Azure Active Directory to access Azure Storage resources.

To be authorized to your Azure Storage account, you must assign the **Storage Blob Data Contributor** role to your user account in the context of either the Storage account, parent resource group or parent subscription.

This command will cache encrypted login information for current user using the OS built-in mechanisms.

Please refer to the examples for more information.


` + environmentVariableNotice

const environmentVariableNotice = `> [!IMPORTANT]
> If you set an environment variable by using the command line, that variable will be readable in your command line history. Consider clearing variables that contain credentials from your command line history. To keep variables from appearing in your history, you can use a script to prompt the user for their credentials, and to set the environment variable.
`
const exampleSnippetStart = "```azcopy"
const exampleSnippetEnd = "```"

const loginCmdExample = `Log in interactively with default AAD tenant ID set to common:

` + exampleSnippetStart + `
azcopy login
` + exampleSnippetEnd + `

Log in interactively with a specified tenant ID:

` + exampleSnippetStart + `
azcopy login --tenant-id "[TenantID]"
` + exampleSnippetEnd + `

Log in by using a VM's system-assigned identity:

` + exampleSnippetStart + `
azcopy login --identity
` + exampleSnippetEnd + `

Log in by using a VM's user-assigned identity with a client ID of the service identity:

` + exampleSnippetStart + `
azcopy login --identity --identity-client-id "[ServiceIdentityClientID]"
` + exampleSnippetEnd + `

Log in using a VM's user-assigned identity with an object ID of the service identity:

` + exampleSnippetStart + `
azcopy login --identity --identity-object-id "[ServiceIdentityObjectID]"
` + exampleSnippetEnd + `

Log in using a VM's user-assigned identity with a resource ID of the service identity:

` + exampleSnippetStart + `
azcopy login --identity --identity-resource-id "/subscriptions/<subscriptionId>/resourcegroups/myRG/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myID"
` + exampleSnippetEnd + `

Log in as a service principal using a client secret. Set the environment variable AZCOPY_SPA_CLIENT_SECRET to the client secret for secret based service principal auth.

` + exampleSnippetStart + `
azcopy login --service-principal
` + exampleSnippetEnd + `

Log in as a service principal using a certificate and password. Set the environment variable AZCOPY_SPA_CERT_PASSWORD to the certificate's password for cert-based service principal authorization.

` + exampleSnippetStart + `
azcopy login --service-principal --certificate-path /path/to/my/cert
` + exampleSnippetEnd + `

Make sure to treat /path/to/my/cert as a path to a PEM or PKCS12 file. AzCopy does not reach into the system cert store to obtain your certificate.

--certificate-path is mandatory when doing cert-based service principal auth.
`

// ===================================== LOGOUT COMMAND ===================================== //
const logoutCmdShortDescription = "Logs out to terminate access to Azure Storage resources."

const logoutCmdLongDescription = `This command will remove all the cached login information for the current user.

`

// ===================================== MAKE COMMAND ===================================== //
const makeCmdShortDescription = "Creates a container/share/filesystem"

const makeCmdLongDescription = `Creates a container or file share represented by the given resource URL.

`

const makeCmdExample = `
` + exampleSnippetStart + `
azcopy make "https://[account-name].[blob,file,dfs].core.windows.net/[top-level-resource-name]"
` + exampleSnippetEnd + `
`

// ===================================== REMOVE COMMAND ===================================== //
const removeCmdShortDescription = "Deletes entities from Azure Storage Blob/File/ADLS Gen2"

const removeCmdLongDescription = ``

const removeCmdExample = `
Remove a single blob with SAS:

` + exampleSnippetStart + `
azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"
` + exampleSnippetEnd + `

Remove an entire virtual directory with a SAS:

` + exampleSnippetStart + `
azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
` + exampleSnippetEnd + `

Remove only the top blobs inside a virtual directory but not its sub-directories:

` + exampleSnippetStart + `
azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false
` + exampleSnippetEnd + `

Remove a subset of blobs in a virtual directory (For example: only jpg and pdf files, or if the blob name is "exactName"):

` + exampleSnippetStart + `
azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --include="*.jpg;*.pdf;exactName"
` + exampleSnippetEnd + `

Remove an entire virtual directory, but exclude certain blobs from the scope (For example: every blob that starts with foo or ends with bar):

` + exampleSnippetStart + `
azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --exclude="foo*;*bar"
` + exampleSnippetEnd + `

Remove a single file from Data Lake Storage Gen2 (include and exclude not supported):

` + exampleSnippetStart + `
azcopy rm "https://[account].dfs.core.windows.net/[container]/[path/to/file]?[SAS]"
` + exampleSnippetEnd + `

Remove a single directory from Data Lake Storage Gen2 (include and exclude not supported):

` + exampleSnippetStart + `
azcopy rm "https://[account].dfs.core.windows.net/[container]/[path/to/directory]?[SAS]"
` + exampleSnippetEnd + `
`

// ===================================== SYNC COMMAND ===================================== //
const syncCmdShortDescription = "Replicates source to the destination location"

const syncCmdLongDescription = `
The last modified times are used for comparison. The file is skipped if the last modified time in the destination is more recent.

The supported pairs are:

- local <-> Azure Blob (either SAS or OAuth authentication can be used)

The sync command differs from the copy command in several ways:

  1. The recursive flag is on by default.
  2. The source and destination should not contain patterns(such as * or ?).
  3. The include and exclude flags can be a list of patterns matching to the file names. Please refer to the example section for illustration.
  4. If there are files or blobs at the destination that aren't present at the source, the user will be prompted to delete them.

     This prompt can be silenced by using the corresponding flags to automatically answer the deletion question.

### Advanced

AzCopy automatically detects the content type of the files when uploading from the local disk, based on the file extension or content (if no extension is specified).

The built-in lookup table is small, but on Unix, it's augmented by the local system's mime.types file(s) if available under one or more of these names:

- /etc/mime.types
- /etc/apache2/mime.types
- /etc/apache/mime.types

On Windows, MIME types are extracted from the registry.

`

const syncCmdExample = `
Sync a single file:

` + exampleSnippetStart + `
azcopy sync "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"
` + exampleSnippetEnd + `

Same as above, but this time, also compute MD5 hash of the file content and save it as the blob's Content-MD5 property:

` + exampleSnippetStart + `
azcopy sync "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --put-md5
` + exampleSnippetEnd + `

Sync an entire directory including its sub-directories (note that recursive is on by default):

` + exampleSnippetStart + `
azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]"
` + exampleSnippetEnd + `

or

` + exampleSnippetStart + `
azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --put-md5
` + exampleSnippetEnd + `

Sync only the top files inside a directory but not its sub-directories:

` + exampleSnippetStart + `
azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false
` + exampleSnippetEnd + `

Sync a subset of files in a directory (For example: only jpg and pdf files, or if the file name is "exactName"):

` + exampleSnippetStart + `
azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --include="*.jpg;*.pdf;exactName"
` + exampleSnippetEnd + `

Sync an entire directory, but exclude certain files from the scope (For example: every file that starts with foo or ends with bar):

` + exampleSnippetStart + `
azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --exclude="foo*;*bar"
` + exampleSnippetEnd + `

> [!NOTE]
> if include/exclude flags are used together, only files matching the include patterns would be looked at, but those matching the exclude patterns would be always be ignored.
`
