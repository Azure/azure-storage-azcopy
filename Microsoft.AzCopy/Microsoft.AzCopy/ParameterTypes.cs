#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Microsoft.AzCopy;

// Flags is a base class for command flags classes to extend--
// It implements the heavy lifting of translating from the class to a dictionary of flags.
// Flags should utilize the Flag attribute to link to AzCopy flags
public abstract class Flags
{
    public abstract Verb AssociatedVerb { get; }
    
    // Global flags
    // Caps the transfer rate, in megabits per second.
    // Moment-by-moment throughput might vary slightly from the cap. If this option is set to zero, or it is omitted, the throughput isn't capped.
    [Flag("cap-mbps")] public float? CapMbps;
    // Define the log verbosity
    [Flag("log-level")] public LogLevel? LogLevel;
    // Define the output verbosity
    [Flag("output-level")] public OutputLevel? OutputLevel;
    // Specifies additional domain suffixes where Azure Active Directory login tokens may be sent.
    // The default is '*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net;*.storage.azure.net'.
    // Any listed here are added to the default. For security, you should only put Microsoft Azure domains here.
    [Flag("trusted-microsoft-suffixes")] public IEnumerable<string>? TrustedMicrosoftSuffixes;


    // todo: should we opt to allow output-type here? My thinking was that we'd read the JSON.
    // todo: ditto to skip-version-check, NuGet is a package manager, and a dep update should do it anyway.
    // todo: Plus, we might not always *have* a latest AzCopy binary. Not to mention I'm not certain there's a universal way to hook into logs in .NET.
    
    public Dictionary<string, string> AdditionalFlags = new();
}

// FlagAttribute is another base class for flags-- It implements a basic, dumb stringifier using ToString.
[System.AttributeUsage(System.AttributeTargets.Field)]
public class FlagAttribute : System.Attribute
{
    public readonly string FlagName;
    
    public FlagAttribute(string flagName)
    {
        FlagName = flagName;
    }

    protected virtual string StringifyInput(object target)
    {
        // Simple implementation
        return target.ToString(); 
    }
    
    internal static Dictionary<string, string> PrepareFlags(object target)
    {
        var result = new Dictionary<string, string>();

        foreach (var field in target.GetType().GetFields(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.GetField))
        {
            var attrs = System.Attribute.GetCustomAttributes(field, typeof(FlagAttribute));
            switch (attrs.Length)
            {
                case 0:
                    continue; // Skip variables without [Flag("<name>")]
                case 1:
                    var flagAttribute = (FlagAttribute)attrs[0];
                    var value = field.GetValue(target);
                    if (value == null)
                        continue; // Don't append defaulted flags
                    
                    result[flagAttribute.FlagName] = flagAttribute.StringifyInput(value);
                    
                    break;
                default:
                    throw new Exception($"Ambiguous match for Flag on field {field.Name}. Include one, and only one Flag attribute at a time.");
            }
        }
        
        return result;
    }
}

[System.AttributeUsage(System.AttributeTargets.Field)]
public class DateTimeFlagAttribute : FlagAttribute
{
    
    public DateTimeFlagAttribute(string flagName) : base(flagName) { }

    protected override string StringifyInput(object target)
    {
        if (target is not DateTime time)
            throw new Exception($"Stringification target must be DateTime, not {target.GetType().Name}");
        
        return time.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
    }
}

[System.AttributeUsage(System.AttributeTargets.Field)]
public class ListFlagAttribute<TValue> : FlagAttribute
{
    private readonly char _separator;

    public ListFlagAttribute(string flagName, char separator = ',') : base(flagName)
    {
        _separator = separator;
    }

    protected override string StringifyInput(object target)
    {
        if (target is not IEnumerable<TValue> list)
            throw new Exception($"Stringification target must be IEnumerable<{typeof(TValue).Name}>, not {target.GetType().Name}");
        
        var result = new StringBuilder();

        foreach (var val in list)
        {
            if (val == null)
                continue;

            result.Append($"{val.ToString()}{_separator}");
        }
        
        return result.ToString().TrimEnd(_separator);
    }
}

[System.AttributeUsage(System.AttributeTargets.Field)]
public class DictionaryFlagAttribute<TKey, TValue> : FlagAttribute
{
    private readonly char _entrySeparator;
    private readonly char _kvSeparator;

    public DictionaryFlagAttribute(string flagName, char kvSeparator = '=', char entrySeparator = '&') : base(flagName)
    {
        _entrySeparator = entrySeparator;
        _kvSeparator = kvSeparator;
    }

    protected override string StringifyInput(object target)
    {
        if (target is not Dictionary<TKey, TValue> dict)
            throw new Exception($"BlobTagsFlagAttribute expects a Dictionary<{typeof(TKey).Name}, {typeof(TValue).Name}> not {target.GetType().Name}");
        
        var result = new StringBuilder();

        foreach (var kv in dict)
        {
            result.Append($"{kv.Key}{_kvSeparator}{kv.Value}{_entrySeparator}");
        }
        
        return result.ToString().TrimEnd(_entrySeparator);
    }
}

[System.AttributeUsage(System.AttributeTargets.Field)]
public class WindowsFileAttributeFlagAttribute : FlagAttribute
{
    private static readonly List<KeyValuePair<WindowsAttributes, string>> AttributeStrings =
        new List<KeyValuePair<WindowsAttributes, string>>()
        {
            (new(WindowsAttributes.ReadOnly, "R")),
            (new(WindowsAttributes.Hidden, "H")),
            (new(WindowsAttributes.System, "S")),
            (new(WindowsAttributes.Archive, "A")),
            (new(WindowsAttributes.Normal, "N")),
            (new(WindowsAttributes.Temporary, "T")),
            (new(WindowsAttributes.Compressed, "C")),
            (new(WindowsAttributes.Offline, "O")),
            (new(WindowsAttributes.NonIndexed, "I")),
            (new(WindowsAttributes.Encrypted, "E")),
        };

    public WindowsFileAttributeFlagAttribute(string flagName) : base(flagName) { }

    protected override string StringifyInput(object target)
    {
        if (target is not WindowsAttributes attr)
            throw new Exception($"Stringification target must be WindowsAttributes, not {target.GetType().Name}");

        var result = new StringBuilder();
        
        foreach (var kv in AttributeStrings.Where(kv => (attr & kv.Key) == kv.Key))
            result.Append($"{kv.Value};");

        return result.ToString().TrimEnd(';');
    }
}

[System.AttributeUsage(System.AttributeTargets.Field)]
public class PermanentDeleteFlagAttribute : FlagAttribute
{
    private static Dictionary<PermanentDelete, string> FlagStrings = new()
    {
        {PermanentDelete.Snapshots, "snapshots"},
        {PermanentDelete.Versions, "Versions"},
        {PermanentDelete.Snapshots | PermanentDelete.Versions, "SnapshotsAndVersions"}
    };

    public PermanentDeleteFlagAttribute() : base("permanent-delete") { }

    protected override string StringifyInput(object target)
    {
        if (target is not PermanentDelete pd)
            throw new Exception($"Stringification target must be PermanentDelete, not {target.GetType().Name}");

        string result;
        if (!FlagStrings.TryGetValue(pd, out result))
            throw new Exception($"{pd} is not a valid permanent delete strategy");

        return result;
    }
}

public enum BlobType
{
    Detect,
    BlockBlob,
    PageBlob,
    AppendBlob
}

public enum BlockBlobTier
{
    Hot,
    Cold,
    Cool,
    Archive
}

public enum PageBlobTier
{
    P10,
    P15,
    P20,
    P30,
    P4,
    P40,
    P50,
    P6
}

public enum Md5ValidationStrictness
{
    NoCheck,
    LogOnly,
    FailIfDifferent,
    FailIfDifferentOrMissing
}

public enum FromTo
{
    BlobBlob,
    BlobBlobFS,
    BlobFSBlob,
    BlobFSBlobFS,
    BlobFSFile,
    BlobFSLocal,
    BlobFile,
    BlobLocal,
    BlobPipe,
    FileBlob,
    FileBlobFS,
    FileFile,
    FileLocal,
    FilePipe,
    GCPBlob,
    LocalBlob,
    LocalBlobFS,
    LocalFile,
    PipeBlob,
    PipeFile,
    S3Blob,
    
    // Remove-only options
    BlobTrash,
    FileTrash,
    BlobFSTrash,
}

[Flags]
public enum WindowsAttributes
{
    None = 0,
    ReadOnly = 1,
    Hidden = 2,
    System = 4,
    Archive = 32,
    Normal = 128,
    Temporary = 256,
    Compressed = 2048,
    Offline = 4096,
    NonIndexed = 8192,
    Encrypted = 16384,
}

public enum Overwrite
{
    True,
    False,
    // Prompt, todo: interface for AzCopy to talk back to this lib. Out of scope currently.
    IfSourceNewer
}

public enum InvalidMetadataStrategy
{
    // ExcludeIfInvalid indicates whenever invalid metadata keys are found, exclude the specific metadata with a warning log.
    ExcludeIfInvalid,
    // FailIfInvalid indicates that the individual file will fail if we hit invalid metadata keys at the source.
    FailIfInvalid,
    // RenameIfInvalid indicates whenever invalid metadata key is found, rename the metadata key and save the metadata with renamed key.
    RenameIfInvalid
}

public enum TrailingDotHandlingStrategy
{
    // 'Enable' by default to treat file share related operations in a safe manner.
    Enable,
    // Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths.
    // This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.).
    Disable
}

public enum LogLevel
{
    Info,
    Warning,
    Error,
    None,
}

public enum OutputLevel
{
    Essential,
    Quiet
}

public enum SnapshotRemovalStrategy
{
    // Include snapshots
    Include,
    // Delete only the snapshots, but keep the root blob.
    Only
}

[Flags]
public enum PermanentDelete
{
    Snapshots = 1,
    Versions = 2,
}

public enum AutoLoginType
{
    // Using a service principal application ID & secret/cert
    SPN,
    // Managed Service Identity
    MSI,
    // Device code auth flow
    // DEVICE // todo: needs user input
}