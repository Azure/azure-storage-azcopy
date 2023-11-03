using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Microsoft.AzCopy;

[JsonConverter(typeof(ResponseValueJsonConverter))]
public abstract class ResponseValue
{
    // RawValue is the raw value inside the string.
    protected string? _value;
    public string RawValue => _value ?? "";

    // Response types must be marked with required attributes, or it will catch everything.
    private static readonly Type[] responseTypes = 
        new Type[] {
            typeof(InitMessage),
            typeof(JobSummary),

            // Dryrun isn't currently handled because it's out of scope.
            // Prompts aren't handled either, due to the same scope.
        };

    public static ResponseValue ParseResponse(string rawValue)
    {
        foreach(var respType in responseTypes)
        {
            try
            {var resp = (ResponseValue?) JsonSerializer.Deserialize(rawValue, respType);

                if (resp == null)
                    continue;

                resp._value = rawValue; // Remember the raw value.

                return resp;
            }
            catch
            { // Continue on to the other types
                continue;
            }
        }

        return new PlaintextResponse(rawValue);
    }
}

public class ResponseValueJsonConverter : JsonConverter<ResponseValue>
{
    public override ResponseValue? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return ResponseValue.ParseResponse(reader.GetString());
    }

    public override void Write(Utf8JsonWriter writer, ResponseValue value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.RawValue);
    }
}


public class PlaintextResponse : ResponseValue {
    public PlaintextResponse() { }

    public PlaintextResponse(string rawValue) => _value = rawValue;
}

public class JobSummary : ResponseValue
{
    [JsonInclude]
    [JsonRequired]
    public string? ErrorMsg;
    [JsonInclude]
    [JsonRequired]
    public Guid JobID;

    // todo: this is in AzCopy for debug purposes, should we include it here?
    //[JsonInclude]
    //public string ActiveConnections;

    // Has enumeration finished?
    [JsonInclude]
    [JsonRequired]
    public bool CompleteJobOrdered;

    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long TotalTransfers;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long FileTransfers;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long FolderPropertyTransfers;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long SymlinkTransfers;

    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long TransfersCompleted;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long FoldersCompleted;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long FoldersFailed;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long TransfersFailed;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long FoldersSkipped;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long TransfersSkipped;

    [JsonInclude] // Includes bytes in retries and failed tx
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long BytesOverWire;
    [JsonInclude] // Does not include bytes in retries and failed tx
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long TotalBytesTransferred;
    [JsonInclude] // # of bytes expected in the job
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public long TotalBytesEnumerated;

    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals)]
    public float PercentComplete;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals)]
    public long AverageIOPS;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals)]
    public long AverageE2EMilliseconds;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals)]
    public float ServerBusyPercentage;
    [JsonInclude]
    [JsonRequired]
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals)]
    public float NetworkErrorPercentage;

    [JsonInclude]
    [JsonRequired]
    public TransferDetail[]? FailedTransfers;
    [JsonInclude]
    [JsonRequired]
    public TransferDetail[]? SkippedTransfers;
    [JsonInclude]
    [JsonRequired]
    public PerfConstraint PerfConstraint;
    [JsonInclude]
    [JsonRequired]
    public PerformanceAdvice[]? PerformanceAdvice;

    [JsonInclude] // Irrelevant in most cases, usually used for benchmark jobs
    [JsonRequired]
    public bool IsCleanupJob;
}

public class InitMessage : ResponseValue
{
    [JsonInclude]
    [JsonRequired]
    public string? LogFileLocation;
    [JsonInclude]
    [JsonRequired]
    public Guid JobID;
    [JsonInclude]
    [JsonRequired]
    public bool IsCleanupJob;
}

public struct TransferDetail
{
    [JsonInclude]
    [JsonRequired]
    [JsonPropertyName("Src")]
    public string Source;
    [JsonInclude]
    [JsonRequired]
    [JsonPropertyName("Dst")]
    public string Destination;
    [JsonInclude]
    [JsonRequired]
    public bool IsFolderProperties;
    [JsonInclude]
    [JsonRequired]
    public TransferStatus TransferStatus;
    [JsonInclude]
    [JsonRequired]
    public long TransferSize;
}

[JsonConverter(typeof(TransferStatusJsonConverter))]
public enum TransferStatus
{
    NotStarted,
    Started,
    Success,
    FolderCreated,
    Restarted,
    Failed,
    BlobTierFailure,
    SkippedEntityAlreadyExists,
    SkippedBlobHasSnapshots,
    TierAvailabilityCheckFailure,
    Cancelled
}

internal class TransferStatusJsonConverter : JsonConverter<TransferStatus>
{
    public override TransferStatus Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return Enum.Parse<TransferStatus>(reader.GetString() ?? throw new Exception($"Null string in transfer status"));
    }

    public override void Write(Utf8JsonWriter writer, TransferStatus value, JsonSerializerOptions options)
    {
        writer.WriteStringValue($"{value}");
    }
}

public struct PerformanceAdvice
{
    [JsonInclude]
    [JsonRequired]
    public string Code;
    [JsonInclude]
    [JsonRequired]
    public string Title;
    [JsonInclude]
    [JsonRequired]
    public string Reason;
    [JsonInclude]
    [JsonRequired]
    public bool PriorityAdvice;
}

public enum PerfConstraint
{
    Unknown,
    Disk,
    Service,
    PageBlobService,
    CPU
}

internal class PerfConstraintJsonConverter : JsonConverter<PerfConstraint>
{
    public override PerfConstraint Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return Enum.Parse<PerfConstraint>(reader.GetString() ?? throw new Exception($"Null string in transfer status"));
    }

    public override void Write(Utf8JsonWriter writer, PerfConstraint value, JsonSerializerOptions options)
    {
        writer.WriteStringValue($"{value}");
    }
}