
# Change Log

## Version 10.12.0

### Bug fixes
1. Fixed the problem of always receiving overwrite prompt on azure files folders.

## Version 10.12.0

### New features
1. Added support for include and exclude regex flags, which allow pattern matching on the entire paths.
2. Added dry run mode for copy, remove, and sync. This feature allows the user to visualize the changes before committing them.
3. For SMB aware locations, preserve-smb-info flag is now true by default.
4. Improved how folder lmts are obtained to allow time-based filters for folders.
5. Added support for ACL copying between HNS enabled accounts. The preserve-smb-permissions flag is now deprecated and has been renamed to preserve-permissions.

### Bug fixes
1. Allow from-to to be set for the remove command.
2. Fixed the problem where resume command did not honor AZCOPY_DEFAULT_SERVICE_API_VERSION.
3. Fixed the new version check.
4. Fixed sync issue on Windows where paths are case-insensitive.
5. Added prompt for invalid characters when importing from S3.
6. Fixed bug where public S3 buckets cannot be listed.
7. Sanitize SAS tokens in JSON output for skipped and failed transfers. 
8. Improved folder property preservation across resumes.

## Version 10.11.0

### New features
1. Improved performance for copying small blobs (with size less than `256MiB`) with [Put Blob from URL](https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob-from-url).
1. Added mirror mode support in sync operation via `mirror-mode` flag. The new mode disables last-modified-time based comparisons and overwrites the conflicting files and blobs at the destination if this flag is set to true.
1. Added flag `disable-auto-decoding` to avoid automatic decoding of URL-encoded illegal characters when uploading from Windows. These illegal characters could have encoded as a result of downloading them onto Windows which does not support them.
1. Support custom mime type mapping via environment variable `AZCOPY_CONTENT_TYPE_MAP`.
1. Output message on the CLI when AzCopy detects a proxy for each domain.
1. Interpret DFS endpoints as Blob endpoint automatically when performing service-to-service copy. 

### Bug fixes
1. Tolerate enumeration errors for Azure Files and not fail the entire job when a directory is deleted/modified during scanning. 
1. Log skipped transfers to the scanning log.
1. Fixed pipe upload by adding missing fields such as Metadata, Blob Index Tags, Client Provided Key, Blob Access Tier, etc.
1. Fixed issue of clean up for the benchmark command.

## Version 10.10.0

### New features
1. Support sync for Local/Blob <-> Azure File.
1. Download to temporary file path (.azDownload-[jobID]-[name]) before renaming to the original path.
1. Support CPK by name and CPK by value.
1. Offer knob to disable application logging (Syslog/Windows Event Log).
1. Trust zonal DNS suffix for OAuth by default.
1. Added include-directory-stub flag for the copy command, to allow copying of blobs with metadata of `hdi_isfolder:true`.
1. Display more fields for the list command, please refer to the help message for example.
1. Provide environment variable to set request try timeout, to allow faster retries.

### Bug fixes
1. Improve job progress updating mechanism to improve scalability for larger jobs.
1. Time limit the container creation step, to avoid hanging and improve UX.
1. Set SMB info/permission again after file upload and copy, to fully preserve the integrity of the permission string and last-write-time.
1. Fixed module import problem for V10.

## Version 10.9.0

### New features
1. Added preview support for importing from GCP Storage to Azure Block Blobs.
1. Added scanning logs which have low output by default but can become verbose if desired to help in debugging.
1. Support preservation of tags when copying blobs.
1. Added last modified time info to the list command.

### Bug fixes
1. Removed unexpected conflict prompt for file share folders with special characters in the name, such as ";".
   
## Version 10.8.0

### New features
1. Added option to [disable parallel blob listing](https://github.com/Azure/azure-storage-azcopy/pull/1263)
1. Added support for uploading [large files](https://github.com/Azure/azure-storage-azcopy/pull/1254/files) upto 4TiB. Please refer the [public documentation](https://docs.microsoft.com/en-us/rest/api/storageservices/create-file) for more information
1. Added support for `include-before`flag. Refer [this](https://github.com/Azure/azure-storage-azcopy/issues/1075) for more information

### Bug fixes

1. Fixed issue [#1246](https://github.com/Azure/azure-storage-azcopy/issues/1246) of security vulnerability in x/text package
1. Fixed issue [share snapshot->share copy](https://github.com/Azure/azure-storage-azcopy/pull/1258) with smb permissions

## Version 10.7.0

### New features
1. Added support for auto-login when performing data commands(copy/sync/list/make/remove). Please refer to our documentation for more info.
1. Added ``blob-tags`` flag for setting [blob index tags](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-index-how-to?tabs=azure-portal) when performing copy command. Please note that we support setting blob tags only when tags are explicitly specified. Refer to the [public documentations](https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#remarks) to know more.

### Bug fixes

1. Fixed issue [#1139](https://github.com/Azure/azure-storage-azcopy/issues/1139) to preserve content-type in service-to-service transfer.
1. Fixed issue to allow snapshot restoring.
1. Fixed issue with setting content-type of an empty file when performing copy command.

### Improvements
1. Added support for setting tier directly at the time of [upload](https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#remarks) API call instead of performing a separate [set tier](https://docs.microsoft.com/en-us/rest/api/storageservices/set-blob-tier) API call.

## Version 10.6.1

### Bug fixes

1. Fix issue [#971](https://github.com/Azure/azure-storage-azcopy/issues/971) with scanning directories on a public container
1. Fix issue with piping where source and destinations were reversed
1. Allow piping to use OAuth login
1. Fix issue where transfers with ``overwrite`` flag set to ``IfSourceNewer`` would work incorrectly
1. Fix issue [#1139](https://github.com/Azure/azure-storage-azcopy/issues/1139), incorrect content type in BlobStorage
1. Issue [#1192](https://github.com/Azure/azure-storage-azcopy/issues/1192), intermittent panic when AzCopy job is abort
1. Fix issue with auto-detected content types for 0 length files

## Version 10.6.0

### New features

1. ``azcopy sync`` now supports the persistence of ACLs between supported resources (Azure Files) using the ``--preserve-smb-permissions`` flag.
1. ``azcopy sync`` now supports the persistence of SMB property info between supported resources (Azure Files) using the ``--preserve-smb-info`` flag. The information that can be preserved is Created Time, Last Write Time and Attributes (e.g. Read Only).
1. Added support for [higher block & blob size](https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#remarks) 
    - For service version ``2019-12-12`` or higher, the block size can now be less than or equal to ``4000 MiB``. The maximum size of a block blob therefore can be ``190.7 TiB (4000 MiB X 50,000 blocks)``
1. Added support for [Blob Versioning](https://docs.microsoft.com/en-us/azure/storage/blobs/versioning-overview)
    - Added ``list-of-versions`` flag (specifies a file where each version id is listed on a separate line) to download/delete versions of a blob from Azure Storage.
    - Download/Delete a version of blob by directly specifying its version id in the source blob URL. 

### Bug fixes

1. Logging input command at ERROR level.

## Version 10.5.1

### New features

- Allow more accurate values for job status in `jobs` commands, e.g. completed with failed or skipped transfers.

### Bug fixes

- Fixed issue with removing blobs with hdi_isfolder=true metadata when the list-of-files flag is used.
- Manually unfurl symbolic links to fix long file path issue on UNC locations.


## Version 10.5.0

### New features

1. Improved scanning performance for most cases by adding support for parallel local and Blob enumeration.
1. Added download support for the benchmark command.
1. A new way to quickly copy only files changed after a certain date/time. The `copy` command now accepts
the parameter `--include-after`. It takes an ISO 8601-formatted date, and will copy only those files that were 
changed on or after the given date/time. When processing large numbers of files, this is faster than `sync` or 
`--overwrite=IfSourceNewer`.  But it does require the user to specify the date to be used.  E.g. `2020-08-19T15:04:00Z` 
for a UTC time, `2020-08-19T15:04` for a time in the local timezone of the machine running Azcopy, 
or `2020-08-19` for midnight (00:00), also in the local timezone. 
1. When detecting content type for common static website files, use the commonly correct values instead of looking them up in the registry.
1. Allow the remove command to delete blob directory stubs which have metadata hdi_isfolder=true.
1. The S3 to Blob feature now has GA support. 
1. Added support for load command on Linux based on Microsoft Avere's CLFSLoad extension.
1. Each job now logs its start time precisely in the log file, using ISO 8601 format.  This is useful if you want to 
use that start date as the `--include-after` parameter to a later job on the same directory. Look for "ISO 8601 START TIME" 
in the log.
1. Stop treating zero-item job as failure, to improve the user experience. 
1. Improved the naming of files being generated in benchmark command, by reversing the digits. 
Doing so allows the names to not be an alphabetic series, which used to negatively impact the performance on the service side.
1. Azcopy can now detect when setting a blob tier would be impossible. If azcopy cannot check the destination account type, a new transfer failure status will be set: `TierAvailabilityCheckFailure`

### Bug fixes

1. Fixed the persistence of last-write-time (as part of SMB info when uploading) for Azure Files. It was using the creation time erroneously.
1. Fixed the SAS timestamp parsing issue.
1. Transfers to the File Service with a read-only SAS were failing because we try listing properties for the parent directories.
The user experience is improved by ignoring this benign error and try creating parent directories directly.
1. Fixed issue with mixed SAS and AD authentication in the sync command.
1. Fixed file creation error on Linux when decompression is turned on.
1. Fixed issue on Windows for files with extended charset such as [%00 - %19, %0A-%0F, %1A-%1F].
1. Enabled recovering from unexpectedEOF error.
1. Fixed issue in which attribute filters does not work if source path contains an asterisk in it.
1. Fixed issue of unexpected upload destination when uploading a whole drive in Windows (e.g. "D:\").
 

## Version 10.4.3

### Bug fixes

1. Fixed bug where AzCopy errored if a filename ended with slash character. (E.g. backslash at end of a Linux filename.)

## Version 10.4.2

### Bug fixes

1. Fixed bug in overwrite prompt for folders.

## Version 10.4.1

### New features

1. Added overwrite prompt support for folder property transfers.
1. Perform proxy lookup when the source is S3.

### Bug fixes

1. When downloading from Azure Files to Windows with the `--preserve-smb-permissions` flag, sometimes 
the resulting permissions were not correct. This was fixed by limiting the concurrent SetNamedSecurityInfo operations.
1. Added check to avoid overwriting the file itself when performing copy operations.

## Version 10.4

### New features

1. `azcopy copy` now supports the persistence of ACLs between supported resources (Windows and Azure Files) using the `--persist-smb-permissions` flag.
1. `azcopy copy` now supports the persistence of SMB property info between supported resources (Windows and Azure Files) 
using the `--persist-smb-info` flag. The information that can be preserved is Created Time, Last Write Time and Attributes (e.g. Read Only).
1. AzCopy can now transfer empty folders, and also transfer the properties of folders. This applies when both the source 
and destination support real folders (Blob Storage does not, because it only supports virtual folders).
1. On Windows, AzCopy can now activate the special privileges `SeBackupPrivilege` and `SeRestorePrivilege`.  Most admin-level 
accounts have these privileges in a deactivated state, as do all members of the "Backup Operators" security group.  
If you run AzCopy as one of those users 
and supply the new flag `--backup`, AzCopy will activate the privileges. (Use an elevated command prompt, if running as Admin).
At upload time, this allows AzCopy to read files 
which you wouldn't otherwise have permission to see. At download time, it works with the `--preserve-smb-permissions` flag
to allow preservation of permissions where the Owner is not the user running AzCopy.  The `--backup` flag will report a failure 
if the privileges cannot be activated.   
1. Status output from AzCopy `copy`, `sync`, `jobs list`, and `jobs status` now contains information about folders.
   This includes new properties in the JSON output of copy, sync, list and jobs status commands, when `--output-type
   json` is used.
1. Empty folders are deleted when using `azcopy rm` on Azure Files.
1. Snapshots of Azure File Shares are supported, for read-only access, in `copy`,`sync` and `list`. To use, add a
   `sharesnapshot` parameter at end of URL for your Azure Files source. Remember to separate it from the existing query
   string parameters (i.e. the SAS token) with a `&`.  E.g.
   `https://<youraccount>.file.core.windows.net/sharename?st=2020-03-03T20%3A53%3A48Z&se=2020-03-04T20%3A53%3A48Z&sp=rl&sv=2018-03-28&sr=s&sig=REDACTED&sharesnapshot=2020-03-03T20%3A24%3A13.0000000Z`
1. Benchmark mode is now supported for Azure Files and ADLS Gen 2 (in addition to the existing benchmark support for
   Blob Storage).
1. A special performance optimization is introduced, but only for NON-recursive cases in this release.  An `--include-pattern` that contains only `*` wildcards will be performance optimized when 
   querying blob storage without the recursive flag. The section before the first `*` will be used as a server-side prefix, to filter the search results more efficiently. E.g. `--include-pattern abc*` will be implemented 
as a prefix search for "abc". In a more complex example, `--include-pattern abc*123`, will be implemented as a prefix search for `abc`, followed by normal filtering for all matches of `abc*123`.  To non-recursively process blobs
contained directly in a container or virtual directory include `/*` at the end of the URL (before the query string).  E.g. `http://account.blob.core.windows.net/container/*?<SAS>`.
1. The `--cap-mbps` parameter now parses floating-point numbers. This will allow you to limit your maximum throughput to a fraction of a megabit per second.

### Special notes

1. A more user-friendly error message is returned when an unknown source/destination combination is supplied
1. AzCopy has upgraded to service revision `2019-02-02`. Users targeting local emulators, Azure Stack, or other private/special
 instances of Azure Storage may need to intentionally downgrade their service revision using the environment variable 
 `AZCOPY_DEFAULT_SERVICE_API_VERSION`. Prior to this release, the default service revision was `2018-03-28`.
1. For Azure Files to Azure Files transfers, --persist-smb-permissions and --persist-smb-info are available on all OS's. 
(But for for uploads and downloads, those flags are only available on Windows.)
1. AzCopy now includes a list of trusted domain suffixes for Azure Active Directory (AAD) authentication. 
   After `azcopy login`, the resulting token will only be sent to locations that appear in the list. The list is:
   `*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net`. 
   If necessary, you can add to the the list with the command-line flag: `--trusted-microsoft-suffixes`. For security,
   you should only add Microsoft Azure domains. 
1. When transferring over a million files, AzCopy will reduces its progress reporting frequency from every 2 seconds to every 2 minutes.   

### Breaking changes

1. To accommodate interfacing with JavaScript programs (and other languages that have similar issue with number precision), 
   all the numbers in the JSON output have been converted to strings (i.e. with quotes around them).
1. The TransferStatus value `SkippedFileAlreadyExists` has been renamed `SkippedEntityExists` and may now be used both 
   for when files are skipped and for when the setting of folder properties is skipped.  This affects the input and 
   output of `azcopy jobs show` and the status values shown in the JSON output format from `copy` and `sync`.
1. The format and content of authentication information messages, in the JSON output format, e.g.
   "Using OAuth token for authentication" has been changed.

### Bug fixes

1. AzCopy can now overwrite even Read-Only and Hidden files when downloading to Windows. (The read-only case requires the use of 
   the new `--force-if-read-only` flag.)
1. Fixed a nil dereference when a prefetching error occurs in a upload
1. Fixed a nil dereference when attempting to close a log file while log-level is none
1. AzCopy's scanning of Azure Files sources, for download or Service to Service transfers, is now much faster.
1. Sources and destinations that are identified by their IPv4 address can now be used. This enables usage with storage
   emulators.  Note that the `from-to` flag is typically needed when using such sources or destinations. E.g. `--from-to
   BlobLocal` if downloading from a blob storage emulator to local disk.
1. Filenames containing the character `:` can now safely be downloaded on Windows and uploaded to Azure Files
1. Objects with names containing `+` can now safely be used in imported S3 object names
1. The `check-length` flag is now exposed in benchmark mode, so that length checking can be turned off for more speed,
   when benchmarking with small file sizes. (When using large file sizes, the overhead of the length check is
   insignificant.)
1. The in-app documentation for Service Principal Authentication has been corrected, to now include the application-id
   parameter.
1. ALL filter types are now disallowed when running `azcopy rm` against ADLS Gen2 endpoints. Previously 
include/exclude patterns were disallowed, but exclude-path was not. That was incorrect. All should have been
disallowed because none (other than include-path) are respected. 
1. Fixed empty page range optimization when uploading Managed Disks. In an edge case, there was previously a risk of data corruption if the user uploaded two different images into the same Managed Disk resource one after the other.
   
## Version 10.3.4

### New features

1. Fixed feature parity issue by adding support for "ifSourceNewer" option on the `overwrite` flag. It serves as a replacement of the '\XO' flag in V8.

### Bug fixes

1. Fixed `jobs clean` command on Windows which was previously crashing when the `with-status` flag was used.

## Version 10.3.3

### New features

1. `azcopy list` is now supported on Azure Files and ADLS Gen 2, in addition to Blob Storage.
1. The `--exclude-path` flag is now supported in the `sync` command.
1. Added new environment variable `AZCOPY_USER_AGENT_PREFIX` to allow a prefix to be appended to the user agent strings.

### Bug fixes

1. Content properties (such as Content-Encoding and Cache-Control) are now included when syncing Blob -> Blob and Azure
   Files -> Azure Files
1. Custom metadata is now included when syncing Blob -> Blob and Azure Files -> Azure Files
1. The `azcopy list` command no longer repeats parts of its output. (Previously it would sometimes repeat itself and show the same blob multiple times in the output.)
1. The `--aad-endpoint` parameter is now visible, instead of hidden. It allows use of Azure Active Directory
   authentication in national clouds (e.g. Azure China).
1. On Windows, AzCopy now caches information about which proxy server should be used, instead of looking it up every
   time. This significantly reduces CPU
   usage when transferring many small files. It also solves a rare bug when transfers got permanently "stuck" with
   one uncompleted file.
1. When uploading to a write-only destination, there is now a clearer error message when the built-in file length check
   fails. The message says how to fix the problem using `--check-length=false`.
1. Size checks on managed disk imports are now clearer, and all run at the start of the import process instead of the end.

## Version 10.3.2

### Bug fixes

1. Jobs could not be cancelled while scanning was still in progress.
1. Downloading large managed disks (8 TB and above) failed with errors.
1. Downloading large page blobs might make no progress for the first 15 or 20 minutes.
1. There was a rare error where the final output could under-report the total number of files in the job. That error has been fixed.
1. When using JSON output mode, the output from the rm command on ADLS Gen2 was inconsistent with the output from other commands
1. After authentication errors, files in progress were not cleaned up (deleted) at the destination. If there was an
   authentication failure during a job (e.g. a SAS token expired while in use) this could result in files being left
   behind that had incomplete contents (even though their size looked correct).
1. The AUTO concurrency option, for automatically tuning concurrency as AzCopy runs, started working too late if scanning (aka enumeration) took a long time. This resulted in reduced throughput when using this setting.
1. It was not possible to access the root of Windows drives with lowercase drive letters. E.g. d:\
1. Service to Service transfers would fail when using environment variable to specify OAuth authentication.
1. Certain errors parsing URLs were not reported clearly.
1. When downloading to NUL (/dev/null on Linux), files of zero length no longer trigger errors. (Downloads to NUL can be used in performance testing and bulk MD5 checking.

## Version 10.3.1

### New features

1. Added helpful deprecation notice for legacy include/exclude flags.
1. Added back request ID at log level INFO.
1. Added back cancel-from-stdin option for partner integration.
1. Added flag to define delete snapshot options for the remove command.

### Bug fix

1. Fixed race condition in shutdown of decompressingWriter.
1. Made progress reporting more accurate.

## Version 10.3.0

### Breaking changes

1. The `*` character is no longer supported as a wildcard in URLs, except for the two exceptions
   noted below. It remains supported in local file paths.
   1. The first execption is that `/*` is still allowed at the very end of the "path" section of a
      URL. This is illustrated by the difference between these two source URLs:
      `https://account/container/virtual?SAS` and 
      `https://account/container/virtualDir/*?SAS`.  The former copies the virtual directory
      `virtualDir` by creating a folder of that name at the destination.  The latter copies the
      _contents_ of `virtual` dir directly into the target without creating a folder named
      "virtualDir".'
   1. The second exception is when you are transferring multiple _whole_ containers (or S3 buckets). You can
      use * as a wildcard in the container or bucket name. 
1. The `--include` and `--exclude` parameters have been replaced by `--include-pattern` and
   `--exclude-pattern` (for filenames) and `--include-path` and `--exclude-path` (for paths,
   including directory and filenames).
   The new parameters have behaviour that is better defined in complex situations (such as
   recursion).  The `*` wildcard is supported in the pattern parameters, but _not_ in the path ones.
1. There have been two breaking changes to the JSON output that is produced if you request
   JSON-formatted output. The `sync` command's output in JSON has changed for consistency reasons,
   and the final message type, for `copy` and `sync` has changed its name from `Exit` to `EndOfJob`.
   Tools using the JSON output format to integrate AzCopy should be aware.
1. If downloading to "null" on Windows the target must now be named "NUL", according to standard
   Windows conventions.  "/dev/null" remains correct on Linux. (This feature can be used to test
   throughput or check MD5s without saving the downloaded data.) 
1. The file format of the (still undocumented) `--list-of-files` parameter is changed.  (It remains
   undocmented because, for simplicity, users are
   encouraged to use the new `--include-pattern` and `--include-path` parameters instead.)

### New features

1. `sync` is supported from Blob Storage to Blob Storage, and from Azure Files to Azure Files.
1. `copy` is supported from Azure Files to Azure Files, and from Blob Storage to Azure Files.
1. Percent complete is displayed as each job runs.
1. VHD files are auto-detected as page blobs.
1. A new benchmark mode allows quick and easy performance benchmarking of your network connection to
   Blob Storage. Run AzCopy with the paramaters `bench --help` for details.  This feature is in
   Preview status.
1. The location for AzCopy's "plan" files can be specified with the environment variable
   `AZCOPY_JOB_PLAN_LOCATION`. (If you move the plan files and also move the log files using the existing
   `AZCOPY_LOG_LOCATION`, then AzCopy will not store anything under your home directory on Linux and
   MacOS.  On Windows AzCopy will keep just one small encrypted file under `c:\users\<username>\.azcopy`)
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
1. Memory usage can be controlled by setting the new environment variable `AZCOPY_BUFFER_GB`.
   Decimal values are supported. Actual usage will be the value specified, plus some overhead. 
1. An extra integrity check has been added: the length of the
   completed desination file is checked against that of the source.
1. When downloading, AzCopy can automatically decompress blobs (or Azure Files) that have a
   `Content-Encoding` of `gzip` or `deflate`. To enable this behaviour, supply the `--decompress`
   parameter.
1. The number of disk files accessed concurrently can be controlled with the new
   `AZCOPY_CONCURRENT_FILES` environment variable. This is an advanced setting, which generally
   should not be modified.  It does not affect the number of HTTP connections, which is still
   controlled by `AZCOPY_CONCURRENCY_VALUE`.
1. The values of key environment variables are listed at the start of the log file.
1. An official Windows 32-bit build is now released, in addition to the usual 64-bit builds for
   Linux, Mac and Windows.
1. If you need to refer a literal `*` in the name of a blob or Azure Files file, e.g. for a blob
   named "\*", escape the `*` using standard URL escaping. To do this, replace the `*` with the following
   character sequence: %2A 

### Bug fixes

1. When an AzCopy job is cancelled with CTRL-C, any partially-updated files are now deleted from
   the destination. Previous releases of AzCopy v10 would just immediately exit, leaving destination files
   potentially containing an unknown mix of old and new data. E.g. if uploading a new version of a file
   over top of an old version, cancellation could result in the file being left with some parts
   containing old data, and some containing new data. This issue affected downloads to local disk and
   uploads to Azure Files, ADLS Gen 2, page blobs and append blobs. The bug did not affect transfers to block
   blobs.
1. If a transfer to a brand-new block blob is cancelled before it completes, the uncommitted blocks are now cleaned up
   immediately. Previous versions would leave them, for automatic garbage collection to delete 7 days later.
1. Long pathnames (over 260 characters) are now supported everywhere on Windows, including on UNC
   shares.
1. Safety is improved in the rare cases where two source files correspond to just one destination file. This can happen
   when transferring to a case-insensitive destination, when the new `--decompress` flag removes an extension but
   there's already a file without the extension, and in very rare cases related to escaping of filenames with illegal
   characters. The bug fix ensures that the single resulting file contains data from only _one_ of the source files.
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
1. Uploading a single file to ADLS Gen 2 works now.
1. The `remove` command no longer hangs when removing blobs that have snapshots. Instead it will fail to 
   delete them, and report the failures clearly.
1. Jobs downloading from ADLS Gen 2 that result in no scheduled transfers will no longer hang.


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
