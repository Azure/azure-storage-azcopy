#nullable enable
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Microsoft.AzCopy;

public enum Verb
{
    Copy,
    // Sync, // todo : currently focusing on supporting operations required by partner team
    Remove,
    // List, // todo : ditto
    // Make, // todo : ditto
    // SetProperties // todo : ditto
}

public struct Parameters
{
    public Flags Flags;
    public string[] Targets; // [0] = Source/List/Delete target; [1] = Destination
    public Env Environment;

    internal string BuildArguments()
    {
        var result = new StringBuilder();

        result.Append(Flags.AssociatedVerb.ToString().ToLower());
        foreach (var target in Targets)
        {
            result.Append(" " + target);
        }

        var flags = FlagAttribute.PrepareFlags(Flags);
        foreach (var flag in flags)
        {
            result.Append($" --{flag.Key}={flag.Value}");
        }

        return result.ToString();
    } 
}

public struct Env
{
    // Inherit the parent executable's environment?
    public bool InheritEnvironment;

    // Overrides where the log files are stored, to avoid filling up a disk.
    [Flag("AZCOPY_LOG_LOCATION")] public string? LogLocation;
    // Overrides where the job plan files (used for tracking and resuming) are stored, to avoid filling up a disk.
    [Flag("AZCOPY_JOB_PLAN_LOCATION")] public string? JobPlanLocation;
    // Overrides how many HTTP connections work on transfers. By default, this number is determined based on the number of logical cores on the machine.
    // Expects an integer or AUTO. AUTO will automatically seek the highest throughput, then hold a static number of routines for the rest of the job.
    [Flag("AZCOPY_CONCURRENCY_VALUE")] public string? ConcurrencyValue;
    // Overrides the (approximate) number of files that are in progress at any one time, by controlling how many files we concurrently initiate transfers for.
    [Flag("AZCOPY_CONCURRENT_FILES")] public uint? ConcurrentFiles;
    // Controls the (max) degree of parallelism used during scanning. Only affects parallelized enumerators, which include Azure Files/Blobs, and local file systems.
    [Flag("AZCOPY_CONCURRENT_SCAN")] public uint? ScanningConcurrency;
    // Applies only when Azure Blobs is the source. Concurrent scanning is faster but employs the hierarchical listing API, which can result in more IOs/cost.
    // Specify 'true' to sacrifice performance but save on cost.
    [Flag("AZCOPY_DISABLE_HIERARCHICAL_SCAN")] public bool? DisableHierarchicalScan;
    // Causes AzCopy to look up file properties on parallel 'threads' when scanning the local file system.
    // The threads are drawn from the pool defined by AZCOPY_CONCURRENT_SCAN.
    // Setting this to true may improve scanning performance on Linux.  Not needed or recommended on Windows.
    [Flag("AZCOPY_PARALLEL_STAT_FILES")] public bool? ParallelStatFiles;
    // Max number of GB that AzCopy should use for buffering data between network and disk. The default is based on machine size.
    [Flag("AZCOPY_BUFFER_GB")] public float? BufferGb;
    // Should throughput for page blobs automatically be adjusted to match Service limits? Default is true. Set to 'false' to disable
    [Flag("AZCOPY_PACE_PAGE_BLOBS")] public bool? PacePageBlobs;
    // Set to false to prevent AzCopy from taking CPU usage into account when auto-tuning its concurrency level (e.g. in the benchmark command)
    [Flag("AZCOPY_TUNE_TO_CPU")] public bool? TuneToCPU;
    // By default AzCopy on Windows will cache proxy server lookups at hostname level (not taking URL path into account).
    // Set to false to disable the cache.
    [Flag("AZCOPY_CACHE_PROXY_LOOKUP")] public bool? CacheProxyLookup;
    // Overrides the Azure service API version so that AzCopy could accommodate custom environments such as Azure Stack.
    [Flag("AZCOPY_DEFAULT_SERVICE_API_VERSION")] public bool? DefaultAPIVersion;
    // Add a prefix to the default AzCopy User Agent, which is used for telemetry purposes. A space is automatically inserted.
    [Flag("AZCOPY_USER_AGENT_PREFIX")] public string? UserAgentPrefix;
    // Set time (in minutes) for how long AzCopy should try to upload files for each request before AzCopy times out.
    [Flag("AZCOPY_REQUEST_TRY_TIMEOUT")] public uint? RequestTryTimeout;
    // Disables logging in Syslog or Windows Event Logger. By default we log to these channels.
    // However, to reduce the noise in Syslog/Windows Event Log, consider setting this environment variable to true.
    [Flag("AZCOPY_DISABLE_SYSLOG")] public bool? DisableSyslog;
    // Location of the file to override default OS mime mapping
    [Flag("AZCOPY_CONTENT_TYPE_MAP")] public string? ContentTypeMap;
    // Configures azcopy to download to a temp path before actual download. True by default.
    [Flag("AZCOPY_DOWNLOAD_TO_TEMP_PATH")] public bool? DownloadToTempPath;
    
    // CPK configuration
    [Flag("CPK_ENCRYPTION_KEY")] public string? CpkEncryptionKey;
    [Flag("CPK_ENCRYPTION_KEY_SHA256")] public string? CpkEncryptionKeySha256;
    
    
    // Azure Auth configuration
    [Flag("AZCOPY_TENANT_ID")] public string? AzureOAuthTenantId;

    [Flag("AZCOPY_SPA_APPLICATION_ID")] public string? AzureServicePrincipalApplicationId;
    [Flag("AZCOPY_SPA_CLIENT_SECRET")] public string? AzureServicePrincipalClientSecret; // secret-based auth
    [Flag("AZCOPY_SPA_CERT_PATH")] public string? AzureServicePrincipalCertificatePath; // cert-based auth
    [Flag("AZCOPY_SPA_CERT_PASSWORD")] public string? AzureServicePrincipalCertificatePassword;

    [Flag("AZCOPY_MSI_CLIENT_ID")] public string? AzureManagedIdentityClientId;
    [Flag("AZCOPY_MSI_RESOURCE_STRING")] public string? AzureManagedIdentityResourceString;
    
    
    [Flag("AZCOPY_AUTO_LOGIN_TYPE")] public AutoLoginType? AzureOAuthAutoLoginType;
    // GCP auth configuration
    [Flag("GOOGLE_APPLICATION_CREDENTIALS")] public string? GcpAccessCredentials;
    // S3 auth configuration
    [Flag("AWS_ACCESS_KEY_ID")] public string? AwsAccessKeyId;
    [Flag("AWS_SECRET_ACCESS_KEY")] public string? AwsSecretAccessKey;
    
    /*
    todo:
    - AZCOPY_SHOW_PERF_STATES ( need output handling )
     */
    
    internal Dictionary<string, string> PrepareEnvironment()
    {
        var flags = FlagAttribute.PrepareFlags(this);

        if (!InheritEnvironment) return flags;
        
        foreach(DictionaryEntry entry in System.Environment.GetEnvironmentVariables())
        {
            if (entry.Key is not string key || entry.Value is not string value)
                continue;

            flags.Add(key, value);
        }

        return flags;
    }
}

public class CopyFlags : Flags
{
    // todo: list of files, list of versions. These *could* just create a file in a temp directory that we point to. For that I need to add cleanup to flags.
    
    // True by default. Places folder sources as subdirectories under the destination.
    [Flag("as-subdir")] public bool? AsSubdirectory;
    // Activates Windows' SeBackupPrivilege for Uploads and SeRestorePrivilege for Downloads,
    // allowing AzCopy to bypass permissions checks to read files and write permissions.
    // Requires that the user account running AzCopy has these permissions (e.g. an Administrator, or a member of the 'Backup Operators' group)
    [Flag("backup")] public bool? Backup;
    // Set tags on blob to categorize data in your storage account
    [DictionaryFlag<string, string>("blob-tags")] public Dictionary<string, string>? BlobTags;
    /* Defines the type of blob at the destination. This is used for uploading blobs and when copying between accounts (default 'Detect'). Valid values include 'Detect',
    'BlockBlob', 'PageBlob', and 'AppendBlob'. When copying between accounts, a value of 'Detect' causes AzCopy to use the type of source blob to determine the type of the destination blob.
    When uploading a file, 'Detect' determines if the file is a VHD or a VHDX file based on the file extension.
    If the file is either a VHD or VHDX file, AzCopy treats the file as a page blob. (default "Detect") */
    [Flag("blob-type")] public BlobType? BlobType;
    // Upload page blob to Azure Storage using this blob tier. (default 'None'). Valid options are P10, P15, P20, P30, P4, P40, P50, P6
    [Flag("page-blob-tier")] public PageBlobTier? PageBlobTier;
    // Upload block blob to Azure Storage using this blob tier. Valid options are Hot, Cold, Cool, Archive.
    [Flag("block-blob-tier")] public BlockBlobTier? BlockBlobTier;
    // Use this block size (specified in MiB) when uploading to Azure Storage, and downloading from Azure Storage.
    // The default value is automatically calculated based on file size. Decimal fractions are allowed (For example: 0.25).
    [Flag("block-size-mb")] public float? BlockSizeMb;
    // Check the length of a file on the destination after the transfer.
    // If there is a mismatch between source and destination, the transfer fails. (default true)
    [Flag("check-length")] public bool? CheckLength;
    // Specifies how strictly MD5 hashes should be validated when downloading. Only available when downloading.
    // Available options: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing. (default 'FailIfDifferent') (default "FailIfDifferent")
    [Flag("check-md5")] public Md5ValidationStrictness? CheckMd5;
    // Provided key will be fetched from Azure Key Vault by the storage service
    [Flag("cpk-by-name")] public string? CpkByName;
    // Provided key will be fetched via environment variables
    [Flag("cpk-by-value")] public bool? CpkByValue;
    // Automatically decompress files when downloading, if their content-encoding indicates that they are compressed.
    // The supported content-encoding values are 'gzip' and 'deflate'. File extensions of '.gz'/'.gzip' or '.zz' aren't necessary,
    // but will be removed if present.
    [Flag("decompress")] public bool? Decompress;
    // False by default to enable automatic decoding of illegal chars on Windows. Can be set to true to disable automatic decoding.
    [Flag("disable-auto-decoding")] public bool? DisableAutoDecoding;
    
    // todo: output this properly. Currently out of scope
    // // Prints the file paths that would be copied by this command. This flag does not copy the actual files.
    // [Flag("dry-run")] public bool? DryRun;
    
    // Follow symbolic links when uploading from local file system.
    [Flag("follow-symlinks")] public bool? FollowSymlinks;
    // If enabled, symlink destinations are preserved as the blob content, rather than uploading the file/folder on the other end of the symlink.
    [Flag("preserve-symlinks")] public bool? PreserveSymlinks;
    // When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the existing file has its read-only attribute set
    [Flag("force-if-read-only")] public bool? ForceIfReadOnly;
    // Specified to nudge AzCopy when resource detection may not work (e.g. piping/emulator/azure stack); Valid FromTo are pairs of Source-Destination words
    // (e.g. BlobLocal, BlobBlob) that specify the source and destination resource types.
    [Flag("from-to")] public FromTo? FromTo;
    // Prevents AzCopy from detecting the content-type based on the extension or content of the file.
    [Flag("no-guess-mime-type")] public bool? NoGuessMimeType;
    // Overwrite the conflicting files and blobs at the destination if this flag is set to true. (default 'true')
    [Flag("overwrite")] public Overwrite? Overwrite;
    // Recursively enumerates subdirectories at the source to discover more files to transfer.
    [Flag("recursive")] public bool? Recursive;
    
    // Filter flags
    // Include/Exclude these paths when copying. This option does not support wildcard characters (*).
    // Checks relative path prefix(For example: myFolder;myFolder/subDirName/file.pdf).
    // When used in combination with account traversal, paths do not include the container name.
    [Flag("exclude-path")] public string? ExcludePath;
    [Flag("include-path")] public string? IncludePath;
    // Include/Exclude these files when copying. This option supports wildcard characters (*).
    [Flag("include-pattern")] public string? IncludePattern;
    [Flag("exclude-pattern")] public string? ExcludePattern;
    // Include/Exclude all the relative path of the files that align with regular expressions. Separate regular expressions with ';'.
    [Flag("include-regex")] public string? IncludeRegex;
    [Flag("exclude-regex")] public string? ExcludeRegex;
    // Include only those files modified on or before/after the given date/time. The value should be in ISO8601 format.
    // If no timezone is specified, the value is assumed to be in the local timezone of the machine running AzCopy.
    // E.g. '2020-08-19T15:04:00Z' for a UTC time, or '2020-08-19' for midnight (00:00) in the local timezone.
    // As of AzCopy 10.5, this flag applies only to files, not folders,
    // so folder properties won't be copied when using this flag with --preserve-smb-info or --preserve-smb-permissions.
    [DateTimeFlag("include-after")] public DateTime? IncludeAfter;
    // Inverse of IncludeAfter
    [DateTimeFlag("include-before")] public DateTime? IncludeBefore;
    // (Windows only) Include/Exclude files whose attributes match the attribute list. For example: A;S;R
    [WindowsFileAttributeFlag("exclude-attributes")] public WindowsAttributes? ExcludeAttributes;
    [WindowsFileAttributeFlag("include-attributes")] public WindowsAttributes? IncludeAttributes;
    // Optionally specifies the type of blob (BlockBlob/ PageBlob/ AppendBlob) to exclude when copying blobs from the container or the account.
    // This flag is only applicable when using Azure Blob Storage as a source
    [ListFlag<BlobType>("exclude-blob-type")] public BlobType[]? ExcludeBlobTypes;

    // Standard HTTP headers to apply to blobs/files uploaded
    [Flag("cache-control")] public string? CacheControl;
    [Flag("content-disposition")] public string? ContentDisposition;
    [Flag("content-encoding")] public string? ContentEncoding;
    [Flag("content-language")] public string? ContentLanguage;
    [Flag("content-type")] public string? ContentType;
    [DictionaryFlag<string, string>("metadata")] public Dictionary<string, string>? Metadata;
    
    // Additional properties to preserve
    // Only available on a download.
    [Flag("preserve-last-modified-time")] public bool? PreserveLastModifiedTime;
    // Only has an effect in downloads, and only when PreservePermissions is used.
    // If true (the default), the file Owner and Group are preserved in downloads.
    // If set to false, PreservePermissions will still preserve ACLs but Owner and Group will be based on the user running AzCopy (default true)
    [Flag("preserve-owner")] public bool? PreserveOwner;
    // False by default. Preserves ACLs between aware resources (Windows and Azure Files, or ADLS Gen 2 to ADLS Gen 2).
    // For Hierarchical Namespace accounts, you will need a container SAS or OAuth token with Modify Ownership and Modify Permissions permissions.
    // For downloads, you will also need the --backup flag to restore permissions where the new Owner will not be the user running AzCopy.
    // This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).
    [Flag("preserve-permissions")] public bool? PreservePermissions;
    // 'Preserves' property info gleaned from stat or statx into object metadata. This applies only on an upload or download from/to Linux.
    [Flag("preserve-posix-properties")] public bool? PreservePosixProperties;
    // Preserves SMB property info (last write time, creation time, attribute bits) between SMB-aware resources (Windows and Azure Files).
    // On windows, this flag will be set to true by default. If the source or destination is a volume mounted on Linux using SMB protocol,
    // this flag will have to be explicitly set to true. Only the attribute bits supported by Azure Files will be transferred;
    // any others will be ignored. This flag applies to both files and folders, unless a file-only filter is specified (e.g. include-pattern).
    // The info transferred for folders is the same as that for files, except for Last Write Time which is never preserved for folders. (default true)
    [Flag("preserve-smb-info")] public bool? PreserveSmbInfo;
    
    // S2S specific behavior
    // Detect if the source file/blob changes while it is being read.
    // (This parameter only applies to service to service copies, because the corresponding check is permanently enabled for uploads and downloads.)
    [Flag("s2s-detect-source-changed")] public bool? S2SDetectSourceChanged;
    // Specifies how invalid metadata keys are handled.
    [Flag("s2s-handle-invalid-metadata")] public InvalidMetadataStrategy? S2SHandleInvalidMetadata;
    // Preserve access tier during service to service copy.
    // Please refer to [Azure Blob storage: hot, cool, and archive access tiers](https://docs.microsoft.com/azure/storage/blobs/storage-blob-storage-tiers)
    // to ensure destination storage account supports setting access tier. In the cases that setting access tier is not supported,
    // please set this to false to bypass copying access tier. (Default true)
    [Flag("s2s-preserve-access-tier")] public bool? S2SPreserveAccessTier;
    // Preserve index tags during service to service transfer from one blob storage to another
    [Flag("s2s-preserve-blob-tags")] public bool? S2SPreserveBlobTags;
    // Preserve full properties during service to service copy.
    // For AWS S3 and Azure File non-single file source, the list operation doesn't return full properties of objects and files.
    // To preserve full properties, AzCopy needs to send one additional request per object or file. (default true)
    [Flag("s2s-preserve-properties")] public bool? S2SPreserveProperties;
    
    
    // Azure Files specific behavior
    // If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation.
    // If the destination does not support trailing dot files (Windows or Blob Storage), AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.
    [Flag("trailing-dot")] public TrailingDotHandlingStrategy? TrailingDot;

    public override Verb AssociatedVerb => Verb.Copy;
}

public class RemoveFlags : Flags
{
    // Provided key will be fetched from Azure Key Vault by the storage service
    [Flag("cpk-by-name")] public string? CpkByName;
    // Provided key will be fetched via environment variables
    [Flag("cpk-by-value")] public bool? CpkByValue;
    // By default, the delete operation fails if a blob has snapshots. Specify a strategy to handle snapshots on blobs.
    [Flag("delete-snapshots")] public SnapshotRemovalStrategy? DeleteSnapshots;
    // When overwriting an existing file on Windows or Azure Files, force the overwrite to work even if the existing file has its read-only attribute set
    [Flag("force-if-read-only")] public bool? ForceIfReadOnly;
    // Specified to nudge AzCopy when resource detection may not work (e.g. piping/emulator/azure stack); Valid FromTo are pairs of Source-Destination words
    // (e.g. BlobLocal, BlobBlob) that specify the source and destination resource types.
    [Flag("from-to")] public FromTo? FromTo;
    // This is a preview feature that PERMANENTLY deletes soft-deleted snapshots/versions.
    [PermanentDeleteFlag] public PermanentDelete? PermanentDelete;
    // Recursively enumerates subdirectories at the source to discover more files to transfer.
    [Flag("recursive")] public bool? Recursive;

    // Filter flags
    // Include/Exclude these paths when copying. This option does not support wildcard characters (*).
    // Checks relative path prefix(For example: myFolder;myFolder/subDirName/file.pdf).
    // When used in combination with account traversal, paths do not include the container name.
    [Flag("exclude-path")] public string? ExcludePath;
    [Flag("include-path")] public string? IncludePath;
    // Include/Exclude these files when copying. This option supports wildcard characters (*).
    [Flag("include-pattern")] public string? IncludePattern;
    [Flag("exclude-pattern")] public string? ExcludePattern;
    // Include only those files modified on or before/after the given date/time. The value should be in ISO8601 format.
    // If no timezone is specified, the value is assumed to be in the local timezone of the machine running AzCopy.
    // E.g. '2020-08-19T15:04:00Z' for a UTC time, or '2020-08-19' for midnight (00:00) in the local timezone.
    // As of AzCopy 10.5, this flag applies only to files, not folders,
    // so folder properties won't be copied when using this flag with --preserve-smb-info or --preserve-smb-permissions.
    [DateTimeFlag("include-after")] public DateTime? IncludeAfter;
    // Inverse of IncludeAfter
    [DateTimeFlag("include-before")] public DateTime? IncludeBefore;
    
    // Azure Files specific behavior
    // If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation.
    // If the destination does not support trailing dot files (Windows or Blob Storage), AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.
    [Flag("trailing-dot")] public TrailingDotHandlingStrategy? TrailingDot;
    
    // todo: dry-run
    // todo: list-of-files list-of-versions
    
    public override Verb AssociatedVerb => Verb.Remove;
}
