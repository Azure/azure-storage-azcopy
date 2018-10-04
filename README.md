# AzCopy v10

## About

AzCopy (v10 Preview) is the next-generation command-line utility designed for copying data to/from Microsoft Azure Blob and File, using simple commands designed for optimal performance. You can copy data between a file system and a storage account, or between storage accounts.

## Features

* Copy data from Azure Blob containers/File shares to File system, and vice versa
* Copy block blobs between two Azure Storage accounts
* Sync a directory in local file system to Azure Blob, or vice versa
* List/Remove files and blobs in a given path
* Supports glob patterns in path, --include and --exclude flags
* Resillient: retries automatically after a failure, and supports resuming after a failed job

## Installation

1. Download the AzCopy executable using one of the following links:
    * [Windows x64](https://aka.ms/downloadazcopy-v10-windows)
    * [Linux x64](https://aka.ms/downloadazcopy-v10-linux)
    * [MacOS x64](https://aka.ms/downloadazcopy-v10-mac)

2. Unzip and get started
    
    * unzip azcopy_linux_amd64_10.0.0.zip
    * cd azcopy_linux_amd64_10.0.0
    * ./azcopy

## Manual

### Authenticating with Azure Storage

AzCopy supports two types of authentication:
* Pre-signed URLs (URLs with Shared Access Signature aka. SAS tokens): Simply generate a SAS token from the Azure Portal, Storage Explorer, or one of the other Azure tools and append to the Blob path (container/virtual directory/blob path).
* Azure Active Directory Authentication : Add your user to the 'Blob Data Contributor' role in the Azure Portal, and log on to AzCopy using `azcopy login`. You can then simply use AzCopy commands without any SAS token appended to the path. e.g. `azcopy cp https://myaccount.blob.core.windows.net/container/data /mydata --recursive`

### Getting started

AzCopy is self-documenting. To list the available commands, run:
```
./azcopy -h
```

To view the help page and examples, run:
```
./azcopy <cmd> -h
```

### Simple command-line syntax
```
# The general syntax
./azcopy <cmd> <arguments> --<flag-name>=<flag-value>
 
 # Example:
./azcopy cp <source path> <destination path> --<flag-name>=<flag-value>
./azcopy cp "/path/to/local" "https://account.blob.core.windows.net/container?sastoken" --recursive=true
 ```

To see more examples:
```
./azcopy cp -h
```

Each transfer operation will create a `Job` for AzCopy to act on. You can view the history of jobs using the following command:
```
./azcopy jobs list
```

The job logs and data are located under the $HOME/.azcopy directory on Linux, and %USERPROFILE%\.azcopy on Windows. You can clear the job data/logs if you wish after AzCopy completes the transfers.

### Copy data to Azure storage
The copy command can be used to transfer data from the source to the destination. The locat can be a:
1. local path
2. URL to Azure Blob/Virtual Directory/Container
3. URL to Azure File/Directory/File Share
```
./azcopy <command> <source path> <destination path>
```

The following command will upload `1file.txt` to the Block Blob at `https://myaccount.blob.core.windows.net/mycontainer/1file.txt`.
```
./azcopy cp /data/1file.txt "https://myaccount.blob.core.windows.net/mycontainer/1file.txt?sastokenhere"
```

The following command will upload all files under `directory1` recursively to the path at `https://myaccount.blob.core.windows.net/mycontainer/directory1`.
```
./azcopy cp /data/directory1 "https://myaccount.blob.core.windows.net/mycontainer/directory1?sastokenhere" --recursive=true
```

The following command will upload all files directly under `directory1` without recursing into sub-directories, to the path at `https://myaccount.blob.core.windows.net/mycontainer/directory1`.
```
./azcopy cp /data/directory1/* "https://myaccount.blob.core.windows.net/mycontainer/directory1?sastokenhere"
```

To upload into File storage, simply change the URI to Azure File URI with corresponding SAS token.

### Copy VHD image to Azure Storage

AzCopy by default uploads data into Block Blobs. However if a source file has `.vhd` extension, AzCopy will default to uploading to a Page Blob. 

### Copy data from Azure to local file systems

The following will download all Blob container contents into the local file system creating the directory `mycontainer` in the destination.
```
./azcopy cp "https://myaccount.blob.core.windows.net/mycontainer?sastokenhere" /data/ --recursive=true
```

The following will download all Blob container contents into the local file system. `mycontainer` directory will not be created in the destination because the globbing pattern looks for all paths inside `mycontainer` in the source rather than the `mycontainer` container itself.
```
./azcopy cp "https://myaccount.blob.core.windows.net/mycontainer/*?sastokenhere" /data/ --recursive=true
```

The following command will download all txt files in the source to the `directory1` path. Note that AzCopy will scan the entire source and filter for `.txt` files. This may take a while when you have thousands/millions of files in the source.
```
./azcopy cp "https://myaccount.blob.core.windows.net/mycontainer/directory1/*.txt?sastokenhere" /data/directory1
```

### Copy data between Azure Storage accounts (currently supports Block Blobs only)

Copying data between two Azure Storage accounts make use of the PutBlockFromURL API, and does not use the client machine's network bandwidth. Data is copied between two Azure Storage servers. AzCopy simply orchestrates the copy operation.
```
./azcopy cp "https://myaccount.blob.core.windows.net/?sastokenhere" "https://myotheraccount.blob.core.windows.net/?sastokenhere" --recursive=true
```

### Advanced Use Cases

#### Configure Concurrency

Set the environment variable `AZCOPY_CONCURRENCY_VALUE` to configure the number of concurrent requests. This is set to 300 by default. Note that this does not equal to 300 parallel connections. Reducing this will limit the bandwidth, and CPU used by AzCopy.

## Troubleshooting and Reporting Issues

### Check Logs for errors

AzCopy creates a log file for all the jobs. Look for clues in the logs to understand the problem. AzCopy will print UPLOADFAILED, COPYFAILED, and DOWNLOADFAILED strings for failures with the paths along with the error reason.

     cat 04dc9ca9-158f-7945-5933-564021086c79.log | grep -i UPLOADFAILED

### View and resume jobs

To view the job stats, run:
```
./azcopy jobs show <job-id>
```

To see the transfers of a specific status(Success or Failed), run:
```
./azcopy jobs show <job-id> --with-status=Failed
```

You can resume a failed/cancelled job using its identifier along with the SAS token(s), which are not persisted for security reasons.
```
./azcopy jobs resume <jobid> --source-sas ?sastokenhere --destination-sas ?sastokenhere
```

### Raise an Issue

Raise an issue on this repository for any feedback or issue encountered.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
