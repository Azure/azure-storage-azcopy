# AzCopy Migration Guide for v8 to v10

## Purpose

This migration guide is intended for AzCopy users who are accustomed to the v8 syntax and are seeking to migrate to v10. This article outlines the key differences between these two versions, maps popular AzCopy v8 commands to their equivalent AzCopy v10 commands, and compares the optional parameters.

## General Usage

**v8**

`azcopy /Source:<source> /Dest:<destination> [parameters]`

**v10**

`azcopy copy '<source>' '<destination>' [parameters]`

**Note** This example shows the `azcopy copy` command, but there are many other commands available to you. To see a complete list, open a command window, and type `azcopy -h`.

## Authentication

### Azure Active Directory (Azure AD)

Use the same command for both v8 and v10.

```azcopy
azcopy login
```

AzCopy encrypts and caches your credentials by using the built-in mechanisms of the operating system.

### Shared Access Signature (SAS)

**v8** - Use the appropriate parameter `/SourceSAS:[SAS]` and/or `/DestSAS:[SAS]`.

```azcopy
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
```

**v10** - Append the SAS token to the source and/or destination URIs.

```azcopy
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer?SAS'
  'C:\MyFolder'
```

### Shared Key

**v8** - Use flag `/SourceKey:[key]` and/or `/DestKey:[key]`.

**v10** - Use Azure AD or SAS.

## Common Command Comparisons

### Download a blob to file

**v8**

```azcopy
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer/myblob
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
```

**v10**

```azcopy
azcopy copy
  'https://myaccount.blob.core.windows.net/mycontainer/myblob?SAS'
  'C:\MyFolder'
```

### Download all blobs from a container to directory

**v8**

```azcopy
azcopy
  /Source:https://myaccount.blob.core.windows.net/mycontainer
  /Dest:C:\MyFolder
  /SourceSAS:[SAS]
  /S
```

**v10**

```azcopy
azcopy copy 
'https://<source-storage-account-name>.blob.core.windows.net/<container-name>?<SAS-token>'
'C:\myDirectory\' 
--recursive
```

## Parameters Table

Task | v8 | v10
------------ | ------------- | -------------
Authenticate | `/SourceKey:<Key>` <br> `/DestKey:<Key>` <br> `/SourceSAS:<SAS>` <br> `/DestSAS:<SAS>` | Append the SAS token to the source and/or destination URI. <br> `'blob URI' + '?' + 'SAS'`
Check log verbosity | `/V:[verbose-log-file]` | `--log-level`
Specify journal file folder (Note: a v10 plan file is similar to a v8 journal file but is not used in the same way) | `/Z:[journal-file-folder]` | Modify environment variable: <br> `AZCOPY_JOB_PLAN_LOCATION`
Specify parameter file | `/@:<parameter-file>` | Run commands in command line
Suppress confirmation prompts | `/Y` | Suppressed by default. To enable, specify parameter: overwrite
Specify number of concurrent operations | `/NC:<number-of-concurrent>` | Modify environment variable: <br> `AZCOPY_CONCURRENCY_VALUE`
Specify source/destination type | `/SourceType:<option>` `/DestType:<option>` Options: blob, file | `--from-to=[enums]` <br> (typically not used)
Upload contents recursively | `/S` | `--recursive`
Match a specific pattern | `/Pattern:<pattern>` | `--include-pattern string` <br> `--exclude-pattern string` <br> `--include-path string` <br> `--exclude-path string`
Create an MD5 hash when uploading data | Always does this | `--put-md5`
Check the MD5 hash when downloading data | `/CheckMD5` | `--check-md5=[option]` <br> Options: NoCheck, LogOnly, FailIfDifferent (default, if MD5 hash exists, it will be checked), FailIfDifferentOrMissing
Retrieve listing | `/L` | `azcopy list`
Set modified time to be same as the source blobs | `/MT` | `--preserve-last-modified-time`
Exclude newer source | `/XN` | Not yet supported
Exclude older source | `/XO` | Use the sync command
Upload archive files/blobs | `/A` | See row below
Set attributes | `/IA:[RASHCNETOI]` <br> `/XA:[RASHCNETOI]` | `--include-attributes string` <br> `--exclude-attributes string`
Copy blobs or files synchronously among two Azure Storage endpoints | `/SyncCopy` | V10 is always synchronous from source to destination (See "Common Questions" below) `/	azcopy copy 'https://<source-storage-account-name>.blob.core.windows.net/<container-name>/<blob-path><SAS-token>' 'https://<destination-storage-account-name>.blob.core.windows.net/<container-name>/<blob-path>'`.
Set content type | `/SetContentType:[content-type]` | `--content-type string`
Set blob type at destination | `/BlobType:<option>` Options: page, block, append | `--blob-type string`
Use specified block size | `/BlockSizeInMb:<block-size-in-mb>` | `--block-size-mb float`
Set file name delimiters | `/Delimiter:<delimiter>` | N/A
Transfer blob snapshots | `/Snapshot` | Coming soon

## Common Questions

### How does service-to-service transfer differ in V8 and V10?
V8 either schedules transfers (async) or downloads the data and re-uploads it (streaming).
V10 uses new synchronous copy APIs where data is read on the server side.

### How can I use AzCopy to work with Azure Tables?
The latest version that supports tables is AzCopy 7.3.

### How can I use AzCopy to work with Azure Queues?
The latest version that supports queues is AzCopy 8.1.

### What is different between the job management (how to resume jobs in both use examples)?
In v8, you can rerun the same command from before and answer the prompt.
In v10, you can have a job sub group where you can resume with job id.
For more information, please visit [this](https://docs.microsoft.com/en-us/azure/storage/common/storage-ref-azcopy-jobs-resume) page

### How can I figure out which files failed?

To learn more about AzCopy v8 visit [this](https://docs.microsoft.com/en-us/previous-versions/azure/storage/storage-use-azcopy) page.
To learn more about AzCopy v10 visit [this](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) page.
