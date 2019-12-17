# Migration Guide for v8 to v10

## Purpose
This migration guide is intended for AzCopy users who are accustomed to the v8 syntax. Below are mappings from AzCopy v8 to their equivalent AzCopy v10 commands. You may use this content to become familiar with using AzCopy v10 and learn how to migrate with ease.

## Usage

v8

`azcopy /Source:<source> /Dest:<destination> [parameters]`

v10

`azcopy copy '<source>' '<destination>' [parameters]`

## Authentication
AzCopy v8 features 3 ways to authenticate credentials outlined below, but for AzCopy v10 we decided to move away from Shared Key due to poor security.

### Azure Active Directory (AAD)

v8/v10: For both versions, this step is the same and will cache the user's encrypted login information using OS built-in mechanisms.
```
azcopy login
```

### Shared Access Signature (SAS)

v8: Use appropriate parameter `/SourceSAS:[SAS]` and/or `/DestSAS:[SAS]`
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
```

v10: Append SAS token to the source and/or destination URIs
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  'C:\MyFolder'
```

### Shared Key

v8: Use flag `/SourceKey:[key]` and/or `/DestKey:[key]`

v10: Use OAuth or SAS

## Command Comparisons

#### Download a blob to file
v8
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer/myblob
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
```
v10
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer/myblob?SAS'
  'C:\MyFolder'
```

#### Download all blobs from a container to directory
v8
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer/myblob
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
  /S
```
v10
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  'C:\MyFolder'
  --recursive
```

#### Upload all blobs in a folder
v8
```
azcopy
  /Source:C:\MyFolder
  /Dest:https://myaccount.blob.core.windows.net/mycontainer
  /DestSAS:[SAS]
  /S
```
v10
```
azcopy copy
  'C:\MyFolder'
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  --recursive
```

#### Copy blobs within storage account (SOMETHING WRONG HERE)
v8
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer1
  /Dest:https://myaccount.blob.core.windows.net/mycontainer2
  /SourceSAS:[SAS1]
  /DestSAS:[SAS2]
```
v10
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer1?SAS1'
  'https://myaccount.blob.core.windows.net/mycontainer2?SAS2'
```

## Parameters Table
Task | v8 | v10
------------ | ------------- | -------------
Authenticate | ```/SourceKey:<Key> /DestKey:<Key> /SourceSAS:<SAS> /DestSAS:<SAS> ``` | Append the SAS token to the source and/or destination URI. `'blob URI' + '?' + 'SAS'`
Check log verbosity | `/V:[verbose-log-file]` | `--log-level`
Specify journal file folder | `/Z:[journal-file-folder]` | Modify environment variable: `AZCOPY_JOB_PLAN_LOCATION`
Specify parameter file | `/@:<parameter-file>` | Run commands in command line
Suppress confirmation prompts | `/Y` | Suppressed by default, to enable specify parameter: INSERT HERE
Specify number of concurrent operations | `/NC:<number-of-concurrent>` | Modify environment variable: `AZCOPY_CONCURRENCY_VALUE`
Specify source/destination type | `/SourceType:<option> /DestType:<option>` Options: blob, file | `--from-to=[enums]` (typically not used)
Upload contents recursively | `/S` | `--recursive`
Match a specific pattern | `/Pattern:<pattern>` | `--include-pattern string --exclude-pattern string --include-path string --exclude-path string`
Create an MD5 hash when downloading data | Always does this | `--put-md5`
Check the MD5 hash when downloading data | `/CheckMD5` | `--check-md5=[option]` Options: NoCheck, LogOnly, FailIfDifferent (default), FailIfDifferentOrMissing
Retrieve listing | `/L` | `azcopy list`
Set modified time to be same as the source blobs | `/MT` | `--preserve-last-modified-time`
Exclude newer source | `/XN` | Not yet supported
Exclude older source | `/XO` | Use the sync command
Upload archive files/blobs | `/A` | See row below
Set attributes | `/IA:[RASHCNETOI] /XA:[RASHCNETOI]` | `--include-attributes string --exclude-attributes string`
Copy blobs or files synchronously among two Azure Storage endpoints | `/SyncCopy` | V10 is always synchronous from source to destination, unlike v8 which downloads then re-uploads
Set content type | `/SetContentType:[content-type]` | `--content-type string`
Set blob type at destination | `/BlobType:<option>` Options: page, block, append | `--blob-type string`
Use specified block size | `/BlockSizeInMb:<block-size-in-mb>` | `--block-size-mb float`
Set file name delimiters | `/Delimiter:<delimiter>` | N/A
Transfer blob snapshots | `/Snapshot` | Coming soon


## Common Questions

#### How does service-to-service transfer differ in V8 and V10?
v8 either schedules transfers (async) or downloads the data and re-uploads it (streaming).
v10 uses new synchronous copy APIs where data is read on the server side.

#### How can I use AzCopy to work with Azure Tables?
The latest version that supports tables is AzCopy 7.3

#### How can I use AzCopy to work with Azure Queues?
The latest version that supports queues is AzCopy 8.1

#### What is different between the job management (how to resume jobs in both use examples)?
v8 rerun the same command from before and answer the prompt.
v10 have a job sub group where you can resume with job id.

#### How can I figure out which files failed?



To learn more about AzCopy v8 visit [this](https://docs.microsoft.com/en-us/previous-versions/azure/storage/storage-use-azcopy) page.
To learn more about AzCopy v10 visit [this](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) page.
