# Change Log

## Version XX.XX.XX

### Bug fix

1. Added error to using Azure Files without a SAS token (invalid auth configuration).
1. AzCopy v10 now outputs a sensible error & warning when attempting to authenticate a storage account business-to-business.
1. `--log-level=none` now drops no logs, and has a listing in `--help`.
1. Fixed bug where piping was not picking up the service version override, making it not work well against Azure Stack.
1. Fixed a timeout when uploading particularly large files to ADLSG2.

### New features

1. Enabled copying from page/block/append blob to another blob of a different type
1. AzCopy now grabs proxy details (sans authentication) from the Windows Registry using `mattn/go-ieproxy`.

### New features

1. SAS tokens are supported on HNS (Hierarchical Namespace/Azure Data Lake Generation 2) Storage Accounts

## Version 10.1.2

### Breaking change

1. Jobs created with earlier releases cannot be resumed with this release. We recommend 
you update to this release only when you have no partially-completed jobs that you want to resume.

### Bug fix

1. Files with `Content-Encoding: gzip` are now downloaded in compressed form. Previous versions tried to save a 
   decompressed version of the file. But they incorrectly truncated it at the original _compressed_ length, so the
   downloaded file was not complete.
   
   By changing AzCopy to save the compressed version, that problem is solved, and Content-MD5 checks now work for such files. (It is 
   assumed that the Content-MD5 hash is the hash of the _compressed_ file.)

### New features

1. Headers for Content-Disposition, Content-Language and Cache-Control can now be set when uploading
files to Blob Storage and to Azure Files. Run `azcopy copy --help` to see command line parameter
information, including those needed to set the new headers.
1. On-screen job summary is output to the log file at end of job, so that the log will include those summary statistics.

## Version 10.1.1

### Bug fixes

1. Fixed typo in local traverser (error handling in walk).
1. Fixed memory alignment issue for atomic functions on 32 bit system.

## Version 10.1.0 (GA)

### Breaking changes

1. The `--block-size` parameter has been replaced by `--block-size-mb`. The old parameter took a number of _bytes_; the
   new one takes a number of Megabytes (MiB).
1. The following command line parameters have been renamed, for clarity
    * `--output` is now `--output-type`
    * `--md5-validation` is now called `--check-md5`
    * `--s2s-source-change-validation` is now called `--s2s-detect-source-changed`
    * `--s2s-invalid-metadata-handle` is is now called `--s2s-handle-invalid-metadata`
    * `--quota` (in the `make` command) is now called `--quota-gb`. Note that the values were always in GB, the new name
      simply clarifies that fact

### New features

1. AzCopy is now able to be configured to use older API versions. This enables (limited) support for Azure Stack.
1. Listing command now shows file sizes.

### Bug fixes

1. AzCopy v10 now works correctly with ADLS Gen 2 folders that contain spaces in their names.
1. When cancelling with CRTL-C, status of in-progress transfers is now correctly recorded.
1. For security, the Service-to-Service (S2S) feature will only work if both the source and destination connections are
   HTTPS.
1. Use of the `--overwrite` parameter is clarified in the in-application help.
1. Fixed incorrect behavior with setting file descriptor limits on platforms including OS X and BSD.
1. On Linux and OS X, log files are now created with same file permissions as all other files created by AzCopy.
1. ThirdPartyNotice.txt is updated.
1. Load DLL in a more secure manner compatible with Go's sysdll registration.
1. Fixed support for relative paths and shorthands.
1. Fixed bug in pattern matching for blob download when recursive is off.

## Version 10.0.9 (Release Candidate)

### Breaking changes

1. For creating MD5 hashes when uploading, version 10.x now has the OPPOSITE default to version
   AzCopy 8.x. Specifically, as of version 10.0.9, MD5 hashes are NOT created by default. To create
   Content-MD5 hashs when uploading, you must now specify `--put-md5` on the command line.

### New features

1. Can migrate data directly from Amazon Web Services (AWS). In this high-performance data path
   the data is read directly from AWS by the Azure Storage service. It does not need to pass through
   the machine running AzCopy. The copy happens syncronously, so you can see its exact progress.  
1. Can migrate data directly from Azure Files or Azure Blobs (any blob type) to Azure Blobs (any
   blob type). In this high-performance data path the data is read directly from the source by the
   Azure Storage service. It does not need to pass through the machine running AzCopy. The copy
   happens syncronously, so you can see its exact progress.  
1. Sync command prompts with 4 options about deleting unneeded files from the target: Yes, No, All or
   None. (Deletion only happens if the `--delete-destination` flag is specified).
1. Can download to /dev/null. This throws the data away - but is useful for testing raw network
   performance unconstrained by disk; and also for validing MD5 hashes in bulk (when run in a cloud
   VM in the same region as the Storage account)

### Bug fixes

1. Fixed memory leak when downloading large files
1. Fixed performance when downloading a single large file
1. Fixed bug with "too many open files" on Linux
1. Fixed memory leak when uploading sparse files (files with big blocks of zeros) to Page Blobs and
   Azure Files.
1. Fixed issue where application crashed after being throttled by Azure Storage Service. (The
   primary fix here is for Page Blobs, but a secondary part of the fix also helps with Block Blobs.)
1. Fixed functionality and usabilty issues with `remove` command
1. Improved performance for short-duration jobs (e.g. those lasting less than a minute)
1. Prevent unnecessary error message that sometimes appeared when cancelling a job
1. Various improvements to the online help and error messages.


## Version 10.0.8:

1. Rewrote sync command to eliminate numerous bugs and improve usability (see wiki for details)
1. Implemented various improvements to memory management
1. Added MD5 validation support (available options: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing)
1. Added last modified time checks for source to guarantee transfer integrity 
1. Formalized outputs in JSON and elevated the output flag to the root level
1. Eliminated outputs to STDERR (for new version notifications), which were causing problems for certain CI systems
1. Improved log format for Windows
1. Optimized plan file sizes
1. Improved command line parameter names as follows (to be consistent with naming pattern of other parameters):
   1. fromTo -> from-to
   1. blobType -> blob-type
   1. excludedBlobType -> excluded-blob-type
   1. outputRaw (in "list" command) -> output
   1. stdIn-enable (reserved for internal use) -> stdin-enable
