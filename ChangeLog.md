
# Change Log

## Version 10.3.0

### Breaking changes

1. (TBC based on outcome of backslash escape vs --strip-top-dir)  The `*` character is no longer supported as a wildcard in URLs.  (It
   remains supported in local file paths.)
   1. The one execption is that `/*` is still allowed at the very end of the "path" section of a
      URL. This is illustrated by the difference between these two source URLs:
      `https://account/container/virtual?SAS` and 
      `https://account/container/virtualDir/*?SAS`.  The former copies the virtual directory
      `virtualDir` by creating a folder of that name at the destination.  The latter copies the
      _contents_ of `virtual` dir directly into the target without creating a folder named "virtualDir".
   1. If you need to refer a literal `*` in the name of a blob, e.g. for a blob named "*", escape it with a backslash as `\*`
1. The `--include` and `--exclude` parameters have been replaced by `--include-pattern` and
   `--exclude-pattern` (for filenames) and `--include-path` and `--exclude-path` (for paths,
   including directory and filenames).
   The new parameters have behaviour that is better defined in complex situations (such as
   recursion).  The `*` wildcard is supported in the pattern parameters, but _not_ in the path ones.
1. The `remove` command now removes blobs that have snapshots (the blob and all the snapshots are
   removed). Previously there was a bug where it would hang if snaphots existed, but now it will
   delete all of them.
1. There have been two breaking changes to the JSON output that is produced if you request
   JSON-formatted output. The `sync` command's output in JSON has changed for consistency reasons,
   and the final message type, for `copy` and `sync` has changed its name from `Exit` to `EndOfJob`.
   Tools using the JSON output format to integrate AzCopy should be aware.
1. If downloading to "null" on Windows the target must now be named "NUL", according to standard
   Windows conventions.  "/dev/null" remains correct on Linux. (This feature can be used to test
   throughput or check MD5s without saving the downloaded data.) 
1. The file format of the (still undocmented) `--list-of-files` parameter is changed.  (It remains
   undocmented because, for simplicity, users are
   encouraged to use the new `--include-pattern` and `--include-path` parameters instead.)

### New features

1. `sync` is supported from Blob Storage to Blob Storage, and from Azure Files to Azure Files.
1. `copy` is supported from Azure Files to Azure Files, and (TBC?) from Blob Storage to Azure Files.
1. Percent complete is displayed as each job runs.
1. VHD files are auto-detected as page blobs.
1. A new benchmark mode allows quick and easy performance benchmarking of your network connection to
   Blob Storage. Run AzCopy with the paramaters `bench --help` for details.  This feature is in
   Preview status.
1. The location for AzCopy's "plan" files can be specified with the environment variable
   `AZCOPY_JOB_PLAN_LOCATION`.
1. Log files and plan files can be cleaned up to save disk space, using AzCopy's new `jobs rm` and
   `jobs clean` commands.
1. When listing jobs with `jobs show`, the status of each job is included in the output.   
1. The `--overwrite` parameter now supports the value of "prompt" to prompt the user on a
   file-by-file basis. (The old values of true and false are also supported.)   
1. The environment variable `AZCOPY_CONCURRENCY_VALUE` can now be set to "AUTO". This is expected to be
    useful for customers with small networks, or those running AzCopy on
   moderately-powered machines and transfer blobs between accounts.  This feature is in preview status.
1. When uploading from Windows, files can be filtered by Windows-specific file attributes (such as
   "Archive", "Hidden" etc)
1. Memory usange can be controlled by setting the new environment variable `AZCOPY_BUFFER_GB`.
   Decimal values are supported. Actual usage will be the value specified, plus some overhead. 
1. An extra integrity check has been added for Service to Service transfers: the length of the
   completed desination file is checked against that of the source.
1. When downloading, AzCopy can automatically decompress blobs (or Azure Files) that have a
   `Content-Encoding` of `gzip` or `deflate`. To enable this behaviour, supply the `--decompress`
   parameter.
1. The number of disk files accessed concurrently can be controlled with the new
   `AZCOPY_CONCURRENT_FILES` environment variable. This is an advanced setting, which generally
   should not be modified.  It does not affect the number of HTTP connections, which is still
   controlled by `AZCOPY_CONCURRENCY_VALUE`.
1. The values of key environment variables are listed at the start of the log file.
1. An official 32-bit build is now released, in addition to the usual 64-bit builds. (TBC: For which OS's?)

### Bug fixes

1. Long pathnames (over 260 characters) are now supported everywhere on Windows, including on UNC
   shares.
1. When supplying a `--content-type` on the command line it's no longer necessary to also specify
   `--no-guess-mime-type`.
1. There is now no hard-coded limit on the number of files that can be processed by the `sync`
   command.  The number that can be processed (without paging of memory to disk) depends only on the
   amount of RAM available.
1. Transfer of sparse page blobs has been improved, so that for many sparse page blobs only the
   populated pages will transferred.  The one exception is blobs which have had a very high number
   of updates, but which still have significant sparse sections.  Those blobs may not be
   transferred optimally in this release. Handling of such blobs will be improved in a future release.
1. Accessing root of drive (e.g. `d:\`) no longer causes an error.
1. On slow networks, there are no longer excessive log messages sent to the Event Log (Windows) and
   SysLog (Linux).
1. If AzCopy can't check whether it's up to date, it will no longer hang. (Previously, it could hang
   if its version check URL, https://aka.ms/azcopyv10-version-metadata, was unreachable due to
   network routing restrictions.)  
1. (TBC: cleanup of in-flight files is improved after a cancellation (CTRL-C))
1. High concurrency values are supported (e.g. over 1000 connections). While these values are seldom
   needed, they are occasionally useful - e.g. for service-to-service transfer of files around 1 MB
   in size.
1. Files skipped due to "overwrite=false" are no longer logged as "failed".
1. Logging is more concise at the default log level.
1. Error message text, returned by Blob and File services, is now included in the log.
1. A log file is created for copy jobs even when there was nothing to copy.
1. In the log, UPLOAD SUCCESSFUL messages now include the name of the successful file.
1. Clear error messages are given to show that AzCopy does not currently support Customer-Provided
   Encryption Keys.
1. On Windows, downloading a filename with characters not supported by the operating system will
   result in those characters being URL-encoded to construct a Windows-compatible filename. The
   encoding process is reversed if the file is uploaded. 
1.  Uploading a single file to ADLS Gen 2 works now.
     

## Version 10.2.1

### Bug fix

1. Fixed outputting error message for SPN login failures.

## Version 10.2.0

### Bug fix

1. Security: fixed signature redaction in logs to include all error types: the log entries for network failures and HTTP errors could include SAS tokens. In previous releases, the SAS tokens were not always redacted correctly and could be written to the AzCopy log file and also to the Windows Event Log or the Linux Syslog. Now, SAS tokens are correctly redacted when logging those errors. Note that errors returned by the Storage service itself - such as authentication errors and bad container names – were already redacted correctly.
1. Added error to using Azure Files without a SAS token (invalid auth configuration).
1. AzCopy v10 now outputs a sensible error & warning when attempting to authenticate a storage account business-to-business.
1. `--log-level=none` now drops no logs, and has a listing in `--help`.
1. Fixed bug where piping was not picking up the service version override, making it not work well against Azure Stack.
1. Fixed a timeout when uploading particularly large files to ADLSG2.
1. Fixed single wildcard match uploads.

### New features

1. Enabled copying from page/block/append blob to another blob of a different type.
1. AzCopy now grabs proxy details (sans authentication) from the Windows Registry using `mattn/go-ieproxy`.
1. Service Principal Authentication is now available under `azcopy login`-- check `azcopy env` for details on client secrets/cert passwords. 
1. SAS tokens are supported on HNS (Hierarchical Namespace/Azure Data Lake Generation 2) Storage Accounts.
1. Added support for custom headers on ADLS Gen 2.
1. Added support for fractional block size for copy and sync.
1. Use different log output for skipped files (so they don't look like failures).
1. Added bandwidth cap (--cap-mbps) to limit AzCopy's network usage, check `azcopy cp -h` for details.
1. Added ADLS Gen2 support for rm command.

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
