# AzCopy Migration Guide for v8 to v10

## Purpose
This migration guide is intended for AzCopy users who are accustomed to the v8 syntax and are seeking to migrate to v10. The content below outlines key differences in the two versions, maps popular AzCopy v8 commands to their equivalent AzCopy v10 commands, and compares the optional parameters.

## General Usage
v8 | v10
------------- | -------------
`azcopy /Source:<source> /Dest:<destination> [parameters]` | `azcopy copy '<source>' '<destination>' [parameters]`

v8

`azcopy /Source:<source> /Dest:<destination> [parameters]`

v10

`azcopy copy '<source>' '<destination>' [parameters]`

## Authentication

#### Azure Active Directory (Azure AD)

For both v8 and v10, this step is the same and will cache the user's encrypted login information using OS built-in mechanisms.
```
azcopy login
```

#### Shared Access Signature (SAS)

**v8** - Use the appropriate parameter `/SourceSAS:[SAS]` and/or `/DestSAS:[SAS]`
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
```

**v10** - Append the SAS token to the source and/or destination URIs
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  'C:\MyFolder'
```

#### Shared Key

**v8** - Use flag `/SourceKey:[key]` and/or `/DestKey:[key]`

**v10** - Use OAuth or SAS

## Common Command Comparisons

#### Download a blob to file
**v8**
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer/myblob
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
```
**v10**
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer/myblob?SAS'
  'C:\MyFolder'
```

#### Download all blobs from a container to directory
**v8**
```
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
  /S
```
**v10**
```
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  'C:\MyFolder'
  --recursive
```

#### Upload blobs with a specific prefix
The following commands upload all blobs that start with `abc` such as `abc.txt`, `abc1.txt`, `vd1\abcd.txt`, etc.
FIX THIS
**v8**
```
azcopy
  /Source:C:\MyFolder
  /Dest:https://myaccount.blob.core.windows.net/mycontainer
  /DestSAS:[SAS]
  /Pattern:abc
  /S
```
**v10**
```
azcopy copy
  'C:\MyFolder'
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  --include-path 'abc'
  --recursive
```

#### Upload specific files
The following commands uploads all files in the `C:\myDirectory\photos` directory and the `C:\myDirectory\documents\myFile.txt` file.
FIX THIS
**v8**
```
azcopy
  /Source:C:\MyFolder
  /Dest:https://myaccount.blob.core.windows.net/mycontainer
  /DestSAS:[SAS]
  /Pattern: PUT CORRECT PATTERN HERE
```
**v10**
```
azcopy copy
  'C:\MyFolder'
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  --include-path 'photos;documents\myFile.txt'
  --recursive
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
Specify source/destination type | `/SourceType:<option>` `/DestType:<option>` Options: blob, file | `--from-to=[enums]` (typically not used)
Upload contents recursively | `/S` | `--recursive`
Match a specific pattern | `/Pattern:<pattern>` | `--include-pattern string --exclude-pattern string --include-path string --exclude-path string`
Create an MD5 hash when downloading data | Always does this | `--put-md5`
Check the MD5 hash when downloading data | `/CheckMD5` | `--check-md5=[option]` Options: NoCheck, LogOnly, FailIfDifferent (default), FailIfDifferentOrMissing
Retrieve listing | `/L` | `azcopy list`
Set modified time to be same as the source blobs | `/MT` | `--preserve-last-modified-time`
Exclude newer source | `/XN` | Not yet supported
Exclude older source | `/XO` | Use the sync command
Upload archive files/blobs | `/A` | See row below
Set attributes | `/IA:[RASHCNETOI]` `/XA:[RASHCNETOI]` | `--include-attributes string` `--exclude-attributes string`
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
