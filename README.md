# AzCopy v10

## About

AzCopy (v10 Preview) is the next-generation command-line utility designed for copying data to/from Microsoft Azure Blob and File, using simple commands designed for optimal performance. You can copy data between a file system and a storage account, or between storage accounts.

## Features

* Copy data from Azure Blob containers/File shares to File system, and vice versa
* Copy block blobs between two Azure Storage accounts
* Sync a directory in local file system to Azure Blob, or vice versa
* List/Remove files and blobs in a given path
* Supports glob patterns in path, and --exclude flags
* Resillient: retries automatically after a failure, and supports resuming after a failed job

## What's new in v10 ? 

* Synchronize a file system up to Azure Blob or vice versa. Use `azcopy sync <source> <destination>`
* Supports Azure Data Lake Storage Gen2. Use `myaccount.dfs.core.windows.net` for the URI to use ADLS Gen2 APIs.
* Supports copying an entire account (Blob service only) to another account. Use `azcopy cp https://myaccount.blob.core.windows.net https://myotheraccount.blob.core.windows.net` which will enumerate all Blob containers and copy to the destination account
* Account to account copy is now using the new Put from URL APIs that will copy the data directly from one storage account to another. No data transfer is needed down to the client where AzCopy runs. Therefore it is significantly faster!
* List/Remove files and blobs in a given path
* Supports glob patterns in path, and --exclude flags
* Every AzCopy run will create a job order, and a related log file. You can view and restart previous jobs using `azcopy jobs` command.
* Improved performance all around!

## Installation

1. Download the AzCopy executable using one of the following links:
    * [Windows x64](https://aka.ms/downloadazcopy-v10-windows) (zip)
    * [Linux x64](https://aka.ms/downloadazcopy-v10-linux) (tar)
    * [MacOS x64](https://aka.ms/downloadazcopy-v10-mac) (zip)

2. Unzip (or untar on Linux) and get started
    
On Linux:
```
wget -O azcopyv10.tar.gz https://aka.ms/downloadazcopy-v10-linux
tar -xzf azcopyv10.tar.gz
cd azcopy_linux_amd64_10.0.8
./azcopy
```

On Windows:
```
Invoke-WebRequest -Uri https://aka.ms/downloadazcopy-v10-windows -OutFile .\azcopyv10.zip
Expand-Archive azcopyv10.zip -DestinationPath .
cd .\azcopy_windows_amd64_10.0.8
.\azcopy.exe
```

## Manual

### Authenticating with Azure Storage

AzCopy supports two types of authentication. See the table below to see which type you need to use.
* **Pre-signed URLs** (URLs with Shared Access Signature aka. **SAS tokens**): Simply generate a SAS token from the Azure Portal, Storage Explorer, or one of the other Azure tools and append to the Blob path (container/virtual directory/blob path). 
* **Azure Active Directory Authentication** : Add your user to the **'Blob Data Contributor'** role in the Azure Portal, and log on to AzCopy using `azcopy login`. To authenticate with MSI, use `azcopy login --identity`. Once logged in, you can  simply use AzCopy commands without any SAS token appended to the path. e.g. `azcopy cp https://myaccount.blob.core.windows.net/container/data /mydata --recursive`

| Azure Storage service  | Supported authentication methods |
| ------------- | ------------- |
| Blob storage  | SAS tokens OR Azure Active Directory Authentication  |
| File storage  | SAS tokens  |
| ADLS Gen2 | Azure Active Directory Authentication |

> :exclamation::exclamation::exclamation:Note a [SAS token](https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1) is *NOT* an account key. SAS tokens are limited in scope and validity, and starts with a question mark which can be appended to a Blob URL. Here is an example: `?sv=2017-11-09&ss=bf&srt=co&sp=rwac&se=2018-11-16T03:59:09Z&st=2018-11-15T19:59:09Z&sip=10.102.166.17&spr=https,http&sig=k8xSm2K3crBbtNpfoxyvh9n%2BMjDTqRk2XpY8JYIAMaA%3D`.

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
./azcopy cp "/mnt/myfile.txt" "https://myaccount.blob.core.windows.net/mycontainer/myfile.txt?sv=2017-11-09&ss=bf&srt=co&sp=rwac&se=2018-11-16T03:59:09Z&st=2018-11-15T19:59:09Z&sip=10.102.166.17&spr=https,http&sig=k8xSm2K3crBbtNpfoxyvh9n%2BMjDTqRk2XpY8JYIAMaA%3D"
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

#### Configure proxy settings
To configure the proxy settings for AzCopy v10, set the environment variable https_proxy using the following command:

```
# For Windows:
set https_proxy=<proxy IP>:<proxy port>
# For Linux:
export https_proxy=<proxy IP>:<proxy port>
# For MacOS
export https_proxy=<proxy IP>:<proxy port>
```

For proxy authentication, use the following format:

```
export https_proxy=<user>:<pass>@<proxy IP>:<proxy port>
# or with a domain:
export https_proxy=<domain>%5C<user>:<pass>@<proxy IP>:<proxy port>
```

#### Authenticate using Service Principal Names

**1**. Create a new app registration [in the azure portal](https://ms.portal.azure.com/#blade/Microsoft_AAD_IAM/ApplicationsListBlade).

**2**. Ensure the app registration is labeled as a Web App/API. The sign-on URL does not matter, simply set it to http://whatever

![Example registration](images/registration.png)

**3**. Add public keys or client secrets under the "keys" section of the application.

![Keys](images/keys.png)

To generate a private & public keypair for AzCopy, perform the following steps in a command line, replacing example-app with your app name:

    4a. Generate a private key
    
    openssl genrsa -out "example-app.key" 2048
    
    4b. Create the certificate
    
    openssl req -new -key "example-app.key" -subj "/CN=example-app" -out "example-app.csr"
    openssl x509 -req -in "example-app.csr" -signkey "example-app.key" -out "example-app.crt" -days 10000
    
    4c. Create the PKCS12 version of the certificate containing the private key
    
    openssl pkcs12 -export -out "example-app.pfx" -inkey "example-app.key" -in "example-app.crt" -passout <password>
    
**4**. Register the app as a contributor to any [resource groups or storage accounts](https://ms.portal.azure.com/#blade/HubsExtension/BrowseResourceGroupBlade/resourceType/Microsoft.Resources%2Fsubscriptions%2FresourceGroups) you plan to use with AzCopy.

![Example role setup](images/role.png)

**5**. Use one of the following sets of command line parameters depending on which type of SPN authentication you plan to use:

Client secrets:

```
./azcopy login --service-principal --application-id <your application ID> --client-secret <your secret>
```

Certificates:

```
./azcopy login --service-principal --application-id <your application ID> --certificate-path <your certificate's path, can be relative> --certificate-password <your certificate's password, not a mandatory parameter>
```

### Configure log location

Set the environment variable 'AZCOPY_LOG_LOCATION' to a directory of your choice where there is plenty of disk space as logs for large data transfers may use up Gigabytes of space depending on the chosen log level.

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

### FAQ

- What is the difference between `sync` and `copy`?
  - The `copy` command is a simple transferring operation, it scans the source and attempts to transfer every single file/blob. The supported source/destination pairs are listed in the help message of the tool. On the other hand, `sync` makes sure that whatever is present in the source will be replicated to the destination, and also whatever is not at the source will be deleted from the destination. If your goal is to simply move some files, then `copy` is definitely the right command, since it offers much better performance.
  - For `sync`, last modified times are used to determine whether to transfer the same file present at both the source and the destination.
  - If the use case is to incrementally transfer data, then `sync` is the better choice, since only the modified/missing files are transferred.
- Will `copy` overwrite my files?
  - By default, AzCopy will overwrite the files at the destination if they already exist. To avoid this behavior, please use the flag `--overwrite`. 

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
