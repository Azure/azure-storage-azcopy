package cmd

import "github.com/Azure/azure-storage-azcopy/v10/common"

// ===================================== ROOT COMMAND ===================================== //
const rootCmdShortDescription = "AzCopy is a command line tool that moves data into and out of Azure Storage."

const rootCmdLongDescription = "AzCopy " + common.AzcopyVersion +
	`
Project URL: github.com/Azure/azure-storage-azcopy

AzCopy is a command line tool that moves data into and out of Azure Storage.
To report issues or to learn more about the tool, go to github.com/Azure/azure-storage-azcopy.

The general format of the commands is: 'azcopy [command] [arguments] --[flag-name]=[flag-value]'.
`

// ===================================== COPY COMMAND ===================================== //
const copyCmdShortDescription = "Copies source data to a destination location"

const copyCmdLongDescription = `
Copies source data to a destination location. The supported directions and forms of authorization are:
  - local <-> Azure Blob (Microsoft Entra ID or Shared access signature (SAS))
  - local <-> Azure Files SMB (Microsoft Entra ID or Share/directory SAS)
  - local <-> Azure Files NFS (Microsoft Entra ID or Share/directory SAS)
  - local <-> Azure Data Lake Storage (Microsoft Entra ID, SAS, or Shared Key)
  - Azure Blob (Microsoft Entra ID, SAS, public) -> Azure Blob (Microsoft Entra ID or SAS)
  - Data Lake Storage (Microsoft Entra ID or SAS) <-> Data Lake Storage (Microsoft Entra ID or SAS)
  - Data Lake Storage (Microsoft Entra ID or SAS) <-> Azure Blob (Microsoft Entra ID or SAS)
  - Azure Blob (Microsoft Entra ID, SAS or public) -> Azure Files SMB (Microsoft Entra ID or SAS)
  - Azure Files SMB (Microsoft Entra ID or SAS) -> Azure Files SMB (Microsoft Entra ID or SAS)
  - Azure Files SMB (Microsoft Entra ID or SAS) -> Azure Blob (Microsoft Entra ID or SAS)
  - Azure Files NFS (Microsoft Entra ID or SAS) -> Azure Files NFS (Microsoft Entra ID or SAS)
  - AWS S3 (Access Key) -> Azure Block Blob (Microsoft Entra ID or SAS)
  - Google Cloud Storage (Service Account Key) -> Azure Block Blob (Microsoft Entra ID or SAS)

Please refer to the examples for more information.

Advanced:
AzCopy does not support modifications to the source or destination during a transfer. 

When you upload files from a local disk, AzCopy automatically detects the content type of the files based on the file extension or content (if no extension is specified).

The built-in lookup table is small, but on Unix, it is augmented by the local system's mime.types file(s) if those files are available under one or more of these names:

- /etc/mime.types
- /etc/apache2/mime.types
- /etc/apache/mime.types

On Windows, MIME types are extracted from the registry. This feature can be turned off with the help of a flag. Please refer to the flag section.

` + environmentVariableNotice

const copyCmdExample = `Upload a single file by using Microsoft Entra ID authorization. 
If you have not yet logged into AzCopy, please run the azcopy login command before you run the following command.

  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Same as above, but this time also compute MD5 hash of the file content and save it as the blob's Content-MD5 property:

  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --put-md5

Upload a single file by using a SAS token:

  - azcopy cp "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Upload a single file by using a SAS token and piping (block blobs only):
  
  - cat "/path/to/file.txt" | azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" 
	--from-to PipeBlob				

Upload a single file by using piping (block blobs only). 
This example assumes that you've authorized access by using Microsoft Entra ID:

  - cat "/path/to/file.txt" | azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]" 
	--from-to PipeBlob

Upload an entire directory by using a SAS token:
  
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" 
	--recursive=true
or
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" 
	--recursive=true --put-md5

Upload a set of files by using a SAS token and wildcard (*) characters:
 
  - azcopy cp "/path/*foo/*bar/*.pdf" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]"

Upload files and directories by using a SAS token and wildcard (*) characters:

  - azcopy cp "/path/*foo/*bar*" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" 
	--recursive=true

Upload files and directories to Azure Storage account and set the query-string encoded tags on the blob. 

	- To set tags {key = "bla bla", val = "foo"} and {key = "bla bla 2", val = "bar"}, use the following syntax :
		- azcopy cp "/path/*foo/*bar*" "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" 
			--blob-tags="bla%20bla=foo&bla%20bla%202=bar"
	- Keys and values are URL encoded and the key-value pairs are separated by an ampersand('&')
	- https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-index-how-to?tabs=azure-portal
	- While setting tags on the blobs, there are additional permissions('t' for tags) in SAS without which the service 
      will give authorization error back.

Download a single file by using Microsoft Entra ID authorization. 
If you have not yet logged into AzCopy, please run the azcopy login command before you run the following command.

  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]" "/path/to/file.txt"

Download a single file by using a SAS token:

  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" "/path/to/file.txt"

Download a single file by using a SAS token and then piping the output to a file (block blobs only):
  
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" 
	--from-to BlobPipe > "/path/to/file.txt"

Download a single file by piping the output to a file (block blobs only). 
This example assumes that you've authorized access by using Microsoft Entra ID
  
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/blob]" 
	--from-to BlobPipe > "/path/to/file.txt"

Download an entire directory by using a SAS token:
  
  - azcopy cp "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" "/path/to/dir" 
	--recursive=true

A note about using a wildcard character (*) in URLs:

There's only two supported ways to use a wildcard character in a URL. 
- You can use one just after the final forward slash (/) of a URL. 
  This copies all of the files in a directory directly to the destination without placing them into a subdirectory. 
- You can also use one in the name of a container as long as the URL refers only to a container and not to a blob. 
  You can use this approach to obtain files from a subset of containers. 

Download the contents of a directory without copying the containing directory itself.
 
  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/folder]/*?[SAS]" "/path/to/dir"

Download an entire storage account.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/" "/path/to/dir" --recursive

Download a subset of containers within a storage account by using a wildcard symbol (*) in the container name.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container*name]" "/path/to/dir" --recursive

Download all the versions of a blob from Azure Storage listed in a text file (i.e, versionidsFile) to local directory. 
Ensure that source is a valid blob, destination is a local folder and versionidsFile is a text file where each version is written on a separate line. 
All the specified versions will get downloaded in the destination folder specified.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[containername]/[blobname]" "/path/to/dir" 
	--list-of-versions="/another/path/to/dir/[versionidsFile]"

Copy a subset of files within a flat container by using a wildcard symbol (*) in the container name 
without listing all files in the container.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[containername]/*" "/path/to/dir" --include-pattern="1*"

Copy a single blob to another blob by using a SAS token.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" 
	"https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy a single blob to another blob by using a SAS token on the source URL and Microsoft Entra ID authorization at the destination. 
You have to use a SAS token at the end of the source account URL if you do not have the right permissions to read it with the identity used for login. 

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" 
	"https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]"

Copy one blob virtual directory to another by using a SAS token:

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" 
	"https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Copy all blob containers, directories, and blobs from storage account to another by using a SAS token:

  - azcopy cp "https://[srcaccount].blob.core.windows.net?[SAS]" 
	"https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy a single object to Blob Storage from AWS S3 by using an access key and a SAS token. 
First, set the environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for AWS S3 source.
  
  - azcopy cp "https://s3.amazonaws.com/[bucket]/[object]" 
	"https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy an entire directory to Blob Storage from AWS S3 by using an access key and a SAS token. 
First, set the environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for AWS S3 source.
 
  - azcopy cp "https://s3.amazonaws.com/[bucket]/[folder]" 
	"https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true
    
    Please refer to https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html 
	to better understand the [folder] placeholder.

Copy all buckets to Blob Storage from AWS by using an access key and a SAS token. 
First, set the environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for AWS S3 source.
 
  - azcopy cp "https://s3.amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy all buckets to Blob Storage from an AWS region by using an access key and a SAS token. 
First, set the environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for AWS S3 source.
 
  - azcopy cp "https://s3-[region].amazonaws.com/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy a subset of buckets by using a wildcard symbol (*) in the bucket name. 
Like the previous examples, you'll need an access key and a SAS token. 
Make sure to set the environment variable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for AWS S3 source.

  - azcopy cp "https://s3.amazonaws.com/[bucket*name]/" "https://[destaccount].blob.core.windows.net?[SAS]" --recursive=true

Copy blobs from one blob storage to another and preserve the tags from source. 
To preserve tags, use the following syntax:
  	
  - azcopy cp "https://[account].blob.core.windows.net/[source_container]/[path/to/directory]?[SAS]" 
	"https://[account].blob.core.windows.net/[destination_container]/[path/to/directory]?[SAS]" --s2s-preserve-blob-tags=true

Transfer files and directories to Azure Storage account and set the given query-string encoded tags on the blob. 

	- To set tags {key = "bla bla", val = "foo"} and {key = "bla bla 2", val = "bar"}, use the following syntax :
		- azcopy cp "https://[account].blob.core.windows.net/[source_container]/[path/to/directory]?[SAS]" 
          "https://[account].blob.core.windows.net/[destination_container]/[path/to/directory]?[SAS]" --blob-tags="bla%20bla=foo&bla%20bla%202=bar"
	- Keys and values are URL encoded and the key-value pairs are separated by an ampersand('&')
	- https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-index-how-to?tabs=azure-portal
  - While setting tags on the blobs, there are additional permissions('t' for tags) in SAS without which the service will give authorization error back.

Copy a single object to Blob Storage from Google Cloud Storage (GCS) by using a service account key and a SAS token. 
First, set the environment variable GOOGLE_APPLICATION_CREDENTIALS for GCS source.
  
  - azcopy cp "https://storage.cloud.google.com/[bucket]/[object]" "https://[destaccount].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Copy an entire directory to Blob Storage from GCS by using a service account key and a SAS token. 
First, set the environment variable GOOGLE_APPLICATION_CREDENTIALS for GCS source.
 
  - azcopy cp "https://storage.cloud.google.com/[bucket]/[folder]" 
    "https://[destaccount].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Copy an entire bucket to Blob Storage from GCS by using a service account key and a SAS token. 
First, set the environment variable GOOGLE_APPLICATION_CREDENTIALS for GCS source.
 
  - azcopy cp "https://storage.cloud.google.com/[bucket]" "https://[destaccount].blob.core.windows.net/?[SAS]" --recursive=true

Copy all buckets to Blob Storage from GCS by using a service account key and a SAS token. 
First, set the environment variables GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT=<project-id> for GCS source
 
  - azcopy cp "https://storage.cloud.google.com/" "https://[destaccount].blob.core.windows.net/?[SAS]" --recursive=true

Copy a subset of buckets by using a wildcard symbol (*) in the bucket name from GCS by using 
a service account key and a SAS token for destination. 
First, set the environment variables GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT=<project-id> for GCS source
 
  - azcopy cp "https://storage.cloud.google.com/[bucket*name]/" "https://[destaccount].blob.core.windows.net/?[SAS]" --recursive=true

To copy files changed before or after the AzCopy job has started, AzCopy provides date/time in the job log in ISO8601 format 
(search for 'ISO 8601 START TIME' in the job log) that can be used with the --include-after and --include-before flags, see examples below. 
This is helpful for incremental copies.

Copy a subset of files modified on or after the given date/time (in ISO8601 format) in 
a container by using the include-after flag.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[containername]?[SAS]" 
	"https://[dstaccount].blob.core.windows.net/[containername]?[SAS]" --include-after='2020-08-19T15:04:00Z''"

Copy a subset of files modified on or before the given date/time (in ISO8601 format) in a container by using the include-before flag.

  - azcopy cp "https://[srcaccount].blob.core.windows.net/[containername]?[SAS]" 
	"https://[dstaccount].blob.core.windows.net/[containername]?[SAS]" --include-before='2020-08-19T15:04:00Z'"
`

// ===================================== ENV COMMAND ===================================== //
const envCmdShortDescription = "Shows the environment variables that you can use to configure the behavior of AzCopy."

const envCmdLongDescription = `Shows the environment variables that you can use to configure the behavior of AzCopy.

` + environmentVariableNotice

// ===================================== JOBS COMMAND ===================================== //
const jobsCmdShortDescription = "Sub-commands related to managing jobs"

const jobsCmdLongDescription = "Sub-commands related to managing jobs."

const jobsCmdExample = "azcopy jobs show [jobID]"

const listJobsCmdShortDescription = "Displays information on all jobs"

const listJobsCmdLongDescription = `
Displays information on all jobs.`

const showJobsCmdShortDescription = "Show detailed information for the given job ID"

const showJobsCmdLongDescription = `
If you provide only a job ID, and not a flag, then this command returns the progress summary only.
The byte counts and percent complete that appears when you run this command reflect only files that are completed in the job. 
They don't reflect partially completed files.
If you set the with-status flag, then only the list of transfers associated with the given status appear.`

const resumeJobsCmdShortDescription = "Resume the existing job with the given job ID."

const resumeJobsCmdLongDescription = `
Resume the existing job with the given job ID.`

const removeJobsCmdShortDescription = "Remove all files associated with the given job ID."

const removeJobsCmdLongDescription = `
Remove all files associated with the given job ID.

Note that you can customize the location where log and plan files are saved. See the env command to learn more.`

const removeJobsCmdExample = "  azcopy jobs rm e52247de-0323-b14d-4cc8-76e0be2e2d44"

const cleanJobsCmdShortDescription = "Remove all log and plan files for all jobs"

const cleanJobsCmdLongDescription = `
Note that you can customize the location where log and plan files are saved. See the env command to learn more.`

const cleanJobsCmdExample = "  azcopy jobs clean --with-status=completed"

// ===================================== LIST COMMAND ===================================== //
const listCmdShortDescription = "List the entities in a given resource"

const listCmdLongDescription = `This command lists accounts, containers, and directories. 
Blob Storage, Azure Data Lake Storage, and File Storage are supported. 
Microsoft Entra ID authorization for Files is currently not supported; please use SAS to authenticate for Files.`

const listCmdExample = "azcopy list [containerURL] --properties [semicolon(;) separated list of attributes " +
	"(LastModifiedTime, VersionId, BlobType, BlobAccessTier, ContentType, ContentEncoding, ContentMD5, LeaseState, LeaseDuration, LeaseStatus) " +
	"enclosed in double quotes (\")]"

// ===================================== LOGIN COMMAND ===================================== //
const loginCmdShortDescription = "Log in to Microsoft Entra ID to access Azure Storage resources."

const loginCmdLongDescription = `To be authorized to your Azure Storage account, 
you must assign the **Storage Blob Data Contributor** role to your user account in the context of either
the Storage account, parent resource group or parent subscription.
This command will cache encrypted login information for current user using the OS built-in mechanisms.
Please refer to the examples for more information.

` + environmentVariableNotice

const environmentVariableNotice = "If you set an environment variable by using the command line, that variable will be readable in your command line history. " +
	"Consider clearing variables that contain credentials from your command line history.  " +
	"To keep variables from appearing in your history, you can use a script to prompt the user for their credentials, and to set the environment variable."

const loginCmdExample = `Log in interactively with default Microsoft Entra tenant ID set to common:
- azcopy login

Log in interactively with a specified tenant ID:

   - azcopy login --tenant-id "[TenantID]"

Log in by using the system-assigned identity of a Virtual Machine (VM):

   - azcopy login --identity

Log in by using the user-assigned identity of a VM and a Client ID of the service identity:
   
   - azcopy login --identity --identity-client-id "[ServiceIdentityClientID]"

Log in by using the user-assigned identity of a VM and an Object ID of the service identity:

   - azcopy login --identity --identity-object-id "[ServiceIdentityObjectID]"

Log in by using the user-assigned identity of a VM and a Resource ID of the service identity:
 
   - azcopy login --identity --identity-resource-id 
     "/subscriptions/<subscriptionId>/resourcegroups/myRG/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myID"

Log in as a service principal by using a client secret:
Set the environment variable AZCOPY_SPA_CLIENT_SECRET to the client secret for secret based service principal auth.

   - azcopy login --service-principal --application-id <your service principal's application ID>

Log in as a service principal by using a certificate and it's password:
Set the environment variable AZCOPY_SPA_CERT_PASSWORD to the certificate's password for cert based service principal auth

   - azcopy login --service-principal --certificate-path /path/to/my/cert --application-id <your service principal's application ID>

   Please treat /path/to/my/cert as a path to a PEM or PKCS12 file-- AzCopy does not reach into the system cert store to obtain your certificate.
   --certificate-path is mandatory when doing cert-based service principal auth.

Log in using a Device:
    Set the environment variable AZCOPY_AUTO_LOGIN_TYPE=DEVICE to initiate Device login.
      - azcopy login
      Please note that you will be provided with a code to authenticate via a web browser. For example, you may see a message like this:
      To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code ABCD12345.

Log in using Managed Identity
    Set the environment variable AZCOPY_AUTO_LOGIN_TYPE to MSI.
    Set additional parameters based on your identity type:
    To use Client ID, set AZCOPY_MSI_CLIENT_ID.
    To use Resource string, set AZCOPY_MSI_RESOURCE_STRING.
      - azcopy login
      Upon successful authentication, you will see messages indicating login 
      with identity succeeded and authenticating to the destination using Microsoft Entra ID.

Subcommand for login to check the login status of your current session.
	- azcopy login status 
`

const loginStatusShortDescription = "Prints if you are currently logged in to your Azure Storage account."

const loginStatusLongDescription = "This command will let you know if you are currently logged in to your Azure Storage account."

// ===================================== LOGOUT COMMAND ===================================== //
const logoutCmdShortDescription = "Log out to terminate access to Azure Storage resources."

const logoutCmdLongDescription = `This command will remove all of the cached login information for the current user.`

// ===================================== MAKE COMMAND ===================================== //
const makeCmdShortDescription = "Create a container or file share."

const makeCmdLongDescription = `Create a container or file share represented by the given resource URL.`

const makeCmdExample = `
  - azcopy make "https://[account-name].[blob,file,dfs].core.windows.net/[top-level-resource-name]"
`

// ===================================== REMOVE COMMAND ===================================== //
const removeCmdShortDescription = "Delete blobs or files from an Azure storage account"

const removeCmdLongDescription = `"Delete blobs or files from an Azure storage account"`

const removeCmdExample = `
Remove a single blob by using a SAS token:

   - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]"

Remove an entire virtual directory by using a SAS token:
 
   - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true

Remove only the blobs inside of a virtual directory, but don't remove any subdirectories or blobs within those subdirectories:

   - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false

Remove a subset of blobs in a virtual directory (For example: remove only jpg and pdf files, or if the blob name is "exactName"):

   - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --include-pattern="*.jpg;*.pdf;exactName"

Remove an entire virtual directory but exclude certain blobs from the scope (For example: every blob that starts with foo or ends with bar):

   - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/directory]?[SAS]" --recursive=true --exclude-pattern="foo*;*bar"

Remove specified version ids of a blob from Azure Storage. 
Ensure that source is a valid blob and versionidsfile which takes in a path to the file where each version is written on a separate line. 
All the specified versions will be removed from Azure Storage.

  - azcopy rm "https://[srcaccount].blob.core.windows.net/[containername]/[blobname]" "/path/to/dir" --list-of-versions="/path/to/dir/[versionidsfile]"

Remove specific blobs and virtual directories by putting their relative paths (NOT URL-encoded) in a text file (i.e., list.txt) using the --list-of-files flag. 
In the text file, each blob and virtual directory is written on a separate line, see file contents below.
The --list-of-files flag may incur performance costs due to additional transactions to retrieve object properties. 
For more details on the APIs AzCopy uses and performing cost estimations, visit
https://aka.ms/AzCopyCostEstimation.

   - azcopy rm "https://[account].blob.core.windows.net/[container]/[path/to/parent/dir]" --recursive=true --list-of-files=/usr/bar/list.txt
   - file content:
	   dir1/dir2
	   blob1
	   blob2

Remove a single file from a Blob Storage account that has a hierarchical namespace (include/exclude not supported):

   - azcopy rm "https://[account].dfs.core.windows.net/[container]/[path/to/file]?[SAS]"

Remove a single directory from a Blob Storage account that has a hierarchical namespace (include/exclude not supported):

   - azcopy rm "https://[account].dfs.core.windows.net/[container]/[path/to/directory]?[SAS]"
`

// ===================================== SYNC COMMAND ===================================== //
const syncCmdShortDescription = "Replicate source to the destination location"

const syncCmdLongDescription = `
The last modified times are used for comparison. The file is skipped if the last modified time in the destination is more recent. 
Alternatively, you can use the --compare-hash flag to transfer only files which differ in their MD5 hash.  
The supported directions and forms of authorization are:

  - Local <-> Azure Blob / Azure File (Microsoft Entra ID or SAS)
  - Azure Blob <-> Azure Blob (Microsoft Entra ID SAS)
  - Azure Data Lake Storage <-> Azure Data Lake Storage (Microsoft Entra ID or SAS)
  - Azure File <-> Azure File (Source must include a SAS or is publicly accessible; SAS authorization should be used for destination)
  - Azure Blob <-> Azure File

The sync command differs from the copy command in several ways:

  1. By default, the recursive flag is true and sync copies all subdirectories. Sync only copies the top-level files inside a directory if the recursive flag is false.
  2. When syncing between virtual directories, add a trailing slash to the path (refer to examples) if there's a blob with the same name as one of the virtual directories.
  3. If the 'deleteDestination' flag is set to true or prompt, then sync will delete files and blobs at the destination that are not present at the source.

Advanced:
AzCopy does not support modifications to the source or destination during a transfer. 

Please note that if you don't specify a file extension, AzCopy automatically detects the content type of the files when uploading from the local disk, based on the file extension or content.

The built-in lookup table is small but on Unix it is augmented by the local system's mime.types file(s) if available under one or more of these names:
  
  - /etc/mime.types
  - /etc/apache2/mime.types
  - /etc/apache/mime.types

On Windows, MIME types are extracted from the registry.

By default, sync works off of the last modified times unless you override that default behavior by using the --compare-hash flag. 
So in the case of Azure File <-> Azure File, the header field Last-Modified is used instead of x-ms-file-change-time, 
which means that metadata changes at the source can also trigger a full copy.
`

const syncCmdExample = `
Sync a single file:

   - azcopy sync "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Same as above, but also compute an MD5 hash of the file content, and then save that MD5 hash as the blob's Content-MD5 property. 

   - azcopy sync "/path/to/file.txt" "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --put-md5

Sync an entire directory including its subdirectories (note that recursive is by default on):

   - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]"
or
  - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --put-md5

Sync only the files inside of a directory but not subdirectories or the files inside of subdirectories:

   - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=false

Sync a subset of files in a directory (For example: only jpg and pdf files, or if the file name is "exactName"):

   - azcopy sync "/path/to/dir" "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" 
     --include-pattern="*.jpg;*.pdf;exactName"

Sync an entire directory but exclude certain files from the scope (For example: every file that starts with foo or ends with bar):

   - azcopy sync "/path/to/dir" 
     "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --exclude-pattern="foo*;*bar"

Sync a single blob:

   - azcopy sync "https://[account].blob.core.windows.net/[container]/[path/to/blob]?[SAS]" 
     "https://[account].blob.core.windows.net/[container]/[path/to/blob]"

Sync a virtual directory:

   - azcopy sync "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]?[SAS]" 
     "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --recursive=true

Sync a virtual directory that has the same name as a blob (add a trailing slash to the path in order to disambiguate):

   - azcopy sync "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]/?[SAS]" 
     "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]/" --recursive=true

Sync an Azure File directory (same syntax as Blob):

   - azcopy sync "https://[account].file.core.windows.net/[share]/[path/to/dir]?[SAS]" 
     "https://[account].file.core.windows.net/[share]/[path/to/dir]" --recursive=true

Note: if include and exclude flags are used together, only files matching the include patterns are used, 
but those matching the exclude patterns are ignored.
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
Runs a performance benchmark by uploading or downloading test data to or from a specified destination. 
For uploads, the test data is automatically generated.

The benchmark command runs the same process as 'copy', except that: 

  - Instead of requiring both source and destination parameters, benchmark takes just one. This is the 
    blob or Data Lake Storage container, or an Azure Files Share that you want to upload to or download from.

  - The 'mode' parameter describes whether AzCopy should test uploads to or downloads from given target. 
    Valid values are 'Upload' and 'Download'. Default value is 'Upload'.

  - For upload benchmarks, the payload is described by command line parameters, which control how many files are auto-generated and 
    how big they are. The generation process takes place entirely in memory. Disk is not used.

  - For downloads, the payload consists of whichever files already exist at the source. (See example below about how to generate
    test files if needed).
  
  - Only a few of the optional parameters that are available to the copy command are supported.
  
  - Additional diagnostics are measured and reported.
  
  - For uploads, the default behavior is to delete the transferred data at the end of the test run.  
    For downloads, the data is never actually saved locally.

Benchmark mode will automatically tune itself to the number of parallel TCP connections that gives 
the maximum throughput. It will display that number at the end. To prevent auto-tuning, set the 
AZCOPY_CONCURRENCY_VALUE environment variable to a specific number of connections. 

All the usual authorization types are supported. However, the most convenient approach for benchmarking upload is typically
to create an empty container with a SAS token and use SAS authorization. (Download mode requires a set of test data to be
present in the target container.)
  
`

const benchCmdExample = `
Run an upload benchmark with default parameters (suitable for benchmarking networks up to 1 Gbps):

   - azcopy bench "https://[account].blob.core.windows.net/[container]?[SAS]"

Run an upload benchmark with a specified block size of 2 MiB and check the length of files after transfer:

   - azcopy bench "https://[account].blob.core.windows.net/[container]?<SAS>" --block-size-mb 2 --check-length

Run a benchmark test that uploads 500 files, each 500 MiB in size, with a log level set to only display errors:

   - azcopy bench "https://[account].blob.core.windows.net/[container]?<SAS>" 
     --file-count 500 --size-per-file 500M --log-level ERROR

Run a benchmark test that uploads 100 files, each 2 GiB in size: 
(suitable for benchmarking on a fast network, e.g. 10 Gbps):'

   - azcopy bench "https://[account].blob.core.windows.net/[container]?[SAS]" --file-count 100 --size-per-file 2G

Same as above, but use 50,000 files, each 8 MiB in size and compute their MD5 hashes 
(in the same way that the --put-md5 flag does this in the copy command). 
The purpose of --put-md5 when benchmarking is to test whether MD5 computation affects throughput for the 
selected file count and size:

   - azcopy bench --mode='Upload' "https://[account].blob.core.windows.net/[container]?[SAS]" 
     --file-count 50000 --size-per-file 8M --put-md5

Run a benchmark test that uploads 1000 files, each 100 KiB in size, and creates folders to divide up the data:

   - azcopy bench "https://[account].blob.core.windows.net/[container]?<SAS>" 
     --file-count 1000 --size-per-file 100K --number-of-folders 5
 
Run a benchmark test that downloads existing files from a target

   - azcopy bench --mode='Download' "https://[account].blob.core.windows.net/[container]?[SAS]"

Run a download benchmark with the default parameters and cap the transfer rate at 500 Mbps:

   - azcopy bench --mode=Download "https://[account].blob.core.windows.net/[container]?<SAS>" --cap-mbps 500

Run an upload that does not delete the transferred files. 
(These files can then serve as the payload for a download test)

   - azcopy bench "https://[account].blob.core.windows.net/[container]?[SAS]" --file-count 100 --delete-test-data=false
`

// ===================================== SET-PROPERTIES COMMAND ===================================== //

const setPropertiesCmdShortDescription = "Given a location, change all the valid system properties of that storage (blob or file)"

const setPropertiesCmdLongDescription = `
Sets properties of Blob, Data Lake Storage, and File storage. The properties currently supported by this command are:

	Blobs -> Tier, Metadata, Tags
	Data Lake Storage -> Tier, Metadata, Tags
	Files -> Metadata

Note: dfs endpoints will be replaced by blob endpoints.
`

const setPropertiesCmdExample = `
Change tier of blob to hot:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --block-blob-tier=hot

Change tier of blob to cold:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --block-blob-tier=cold

Change tier of blob from hot to Archive:
 - azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --block-blob-tier=archive

Change tier of blob from archive to cool with rehydrate priority set to high:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --block-blob-tier=cool --rehydrate-priority=high

Change tier of blob from cool to hot with rehydrate priority set to standard: 
  - azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --block-blob-tier=hot --rehydrate-priority=standard
 
Change tier of all files in a directory to archive:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --block-blob-tier=archive --recursive=true

Change tier of a page blob:
 - azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --page-blob-tier=[P10/P15/P20/P30/P4/P40/P50/P6]--rehydrate-priority=[Standard/High]

Change metadata of blob to {key = "abc", val = "def"} and {key = "ghi", val = "jkl"}:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --metadata=abc=def;ghi=jkl

Change metadata of all files in a directory to {key = "abc", val = "def"} and {key = "ghi", val = "jkl"}:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/virtual/dir]" --metadata=abc=def;ghi=jkl --recursive=true

Clear all existing metadata of blob:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --metadata=clear

Clear all existing metadata from all files:
 - azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --recursive --metadata=clear

Change blob-tags of blob to {key = "abc", val = "def"} and {key = "ghi", val = "jkl"}:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --blob-tags=abc=def&ghi=jkl
	- While setting tags on the blobs, there are additional permissions('t' for tags) in SAS without which the service will give authorization error back.

Clear all existing blob-tags of blob:
	- azcopy set-properties "https://[account].blob.core.windows.net/[container]/[path/to/blob]" --blob-tags=clear
	- While setting tags on the blobs, there are additional permissions('t' for tags) in SAS without which the service will give authorization error back.
`
