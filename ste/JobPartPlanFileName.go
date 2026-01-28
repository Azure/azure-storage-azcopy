package ste

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type JobPartPlanFileName string

func (jppfn *JobPartPlanFileName) Exists() bool {
	_, err := os.Stat(jppfn.GetJobPartPlanPath())
	return err == nil
}

func (jppfn *JobPartPlanFileName) GetJobPartPlanPath() string {
	if common.AzcopyJobPlanFolder != "" {
		return fmt.Sprintf("%s%s%s", common.AzcopyJobPlanFolder, common.AZCOPY_PATH_SEPARATOR_STRING, string(*jppfn))
	} else {
		return string(*jppfn)
	}
}

const JobPartPlanFileNameFormat = "%v--%05d.steV%d"

// TODO: This needs testing
func (jpfn JobPartPlanFileName) Parse() (jobID common.JobID, partNumber common.PartNumber, err error) {
	var dataSchemaVersion common.Version
	// n, err := fmt.Sscanf(string(jpfn), jobPartPlanFileNameFormat, &jobID, &partNumber, &dataSchemaVersion)
	// if err != nil || n != 3 {
	//	panic(err)
	// }
	// if dataSchemaVersion != DataSchemaVersion {
	//	err = fmt.Errorf("job part Plan file's data schema version ('%d') doesn't match whatthis app requires ('%d')", dataSchemaVersion, DataSchemaVersion)
	// }
	// TODO: confirm the alternative approach. fmt.Sscanf not working for reading back string into struct JobId.
	jpfnSplit := strings.Split(string(jpfn), "--")
	jobId, err := common.ParseJobID(jpfnSplit[0])
	if err != nil {
		err = fmt.Errorf("failed to parse the JobId from JobPartFileName %s. Failed with error %w", string(jpfn), err) //nolint:staticcheck
		common.GetLifecycleMgr().Warn(err.Error())
	}
	jobID = jobId
	n, err := fmt.Sscanf(jpfnSplit[1], "%05d.steV%d", &partNumber, &dataSchemaVersion)
	if err != nil || n != 2 {
		panic(err)
	}
	if dataSchemaVersion != DataSchemaVersion {
		err = fmt.Errorf("job part Plan file's data schema version ('%d') doesn't match whatthis app requires ('%d')", dataSchemaVersion, DataSchemaVersion)
	}
	return
}

func (jpfn JobPartPlanFileName) Delete() error {
	return os.Remove(string(jpfn))
}

func (jpfn JobPartPlanFileName) Map() *JobPartPlanMMF {
	// opening the file with given filename
	file, err := os.OpenFile(jpfn.GetJobPartPlanPath(), os.O_RDWR, common.DEFAULT_FILE_PERM)
	common.PanicIfErr(err)
	// Ensure the file gets closed (although we can continue to use the MMF)
	defer file.Close()

	fileInfo, err := file.Stat()
	common.PanicIfErr(err)
	mmf, err := common.NewMMF(file, true, 0, fileInfo.Size())
	common.PanicIfErr(err)
	return (*JobPartPlanMMF)(mmf)
}

// createJobPartPlanFile creates the memory map JobPartPlanHeader using the given JobPartOrder and JobPartPlanBlobData
func (jpfn JobPartPlanFileName) Create(order common.CopyJobPartOrderRequest) {
	if jpfn.Exists() {
		panic(fmt.Sprint("Duplicate job created. You probably shouldn't ever see this, but if you do, try cleaning out", jpfn.GetJobPartPlanPath()))
	}

	// Validate that the passed-in strings can fit in their respective fields
	if len(order.SourceRoot.Value) > len(JobPartPlanHeader{}.SourceRoot) {
		panic(fmt.Errorf("source root string is too large: %q", order.SourceRoot))
	}
	if len(order.SourceRoot.ExtraQuery) > len(JobPartPlanHeader{}.SourceExtraQuery) {
		panic(fmt.Errorf("source extra query strings too large: %q", order.SourceRoot.ExtraQuery))
	}
	if len(order.DestinationRoot.Value) > len(JobPartPlanHeader{}.DestinationRoot) {
		panic(fmt.Errorf("destination root string is too large: %q", order.DestinationRoot))
	}
	if len(order.DestinationRoot.ExtraQuery) > len(JobPartPlanHeader{}.DestExtraQuery) {
		panic(fmt.Errorf("destination extra query strings too large: %q", order.DestinationRoot.ExtraQuery))
	}
	if len(order.BlobAttributes.ContentType) > len(JobPartPlanDstBlob{}.ContentType) {
		panic(fmt.Errorf("content type string is too large: %q", order.BlobAttributes.ContentType))
	}
	if len(order.BlobAttributes.ContentEncoding) > len(JobPartPlanDstBlob{}.ContentEncoding) {
		panic(fmt.Errorf("content encoding string is too large: %q", order.BlobAttributes.ContentEncoding))
	}
	if len(order.BlobAttributes.ContentLanguage) > len(JobPartPlanDstBlob{}.ContentLanguage) {
		panic(fmt.Errorf("content language string is too large: %q", order.BlobAttributes.ContentLanguage))
	}
	if len(order.BlobAttributes.ContentDisposition) > len(JobPartPlanDstBlob{}.ContentDisposition) {
		panic(fmt.Errorf("content disposition string is too large: %q", order.BlobAttributes.ContentDisposition))
	}
	if len(order.BlobAttributes.CacheControl) > len(JobPartPlanDstBlob{}.CacheControl) {
		panic(fmt.Errorf("cache control string is too large: %q", order.BlobAttributes.CacheControl))
	}
	if len(order.BlobAttributes.Metadata) > len(JobPartPlanDstBlob{}.Metadata) {
		panic(fmt.Errorf("metadata string is too large: %q", order.BlobAttributes.Metadata))
	}
	if len(order.BlobAttributes.BlobTagsString) > len(JobPartPlanDstBlob{}.BlobTags) {
		panic(fmt.Errorf("blob tags string is too large: %q", order.BlobAttributes.BlobTagsString))
	}

	// This nested function writes a structure value to an io.Writer & returns the number of bytes written
	writeValue := func(writer io.Writer, v interface{}) int64 {
		rv := reflect.ValueOf(v)
		structSize := reflect.TypeOf(v).Elem().Size()
		byteSlice := unsafe.Slice((*byte)(rv.UnsafePointer()), int(structSize))
		err := binary.Write(writer, binary.LittleEndian, byteSlice)
		common.PanicIfErr(err)
		return int64(structSize)
	}

	eof := int64(0)
	/*
	*       Following Steps are executed:
	*		1. Get File Name from JobId and Part Number
	*		2. Create the File with filename
	*       3. Create Job Part Plan From Job Part Order
	*       4. Write Data to file
	* 		5. Close the file
	* 		6. Return File Name
	 */

	// create the Job Part Plan file
	// planPathname := planDir + "/" + string(jpfn)
	file, err := os.Create(jpfn.GetJobPartPlanPath())
	if err != nil {
		panic(fmt.Errorf("couldn't create job part plan file %q: %w", jpfn, err))
	}
	defer file.Close()

	// If block size from the front-end is set to 0
	// store the block-size as 0. While getting the transfer Info
	// auto correction logic will apply. If the block-size stored is not 0
	// it means that user provided some block-size and  auto-correct will not
	// apply.
	blockSize := order.BlobAttributes.BlockSizeInBytes
	// if blockSize == 0 { // TODO: Fix below
	//	blockSize = common.DefaultBlockBlobBlockSize
	//	/*switch order.BlobAttributes.BlobType {
	//	case common.BlobType{}.Block():
	//		blockSize = common.DefaultBlockBlobBlockSize
	//	case common.BlobType{}.Append():
	//		blockSize = common.DefaultAppendBlobBlockSize
	//	case common.BlobType{}.Page():
	//		blockSize = common.DefaultPageBlobChunkSize
	//	default:
	//		panic(errors.New("unrecognized blob type"))
	//	}*/
	// }
	putBlobSize := order.BlobAttributes.PutBlobSizeInBytes
	// Initialize the Job Part's Plan header
	jpph := JobPartPlanHeader{
		Version:                DataSchemaVersion,
		StartTime:              time.Now().UnixNano(),
		JobID:                  order.JobID,
		PartNum:                order.PartNum,
		SourceRootLength:       uint16(len(order.SourceRoot.Value)),
		SourceExtraQueryLength: uint16(len(order.SourceRoot.ExtraQuery)),
		DestinationRootLength:  uint16(len(order.DestinationRoot.Value)),
		DestExtraQueryLength:   uint16(len(order.DestinationRoot.ExtraQuery)),
		IsFinalPart:            order.IsFinalPart,
		ForceWrite:             order.ForceWrite,
		ForceIfReadOnly:        order.ForceIfReadOnly,
		AutoDecompress:         order.AutoDecompress,
		Priority:               order.Priority,
		TTLAfterCompletion:     uint32(time.Time{}.Nanosecond()),
		FromTo:                 order.FromTo,
		Fpo:                    order.Fpo,
		CommandStringLength:    uint32(len(order.CommandString)),
		NumTransfers:           uint32(len(order.Transfers.List)),
		LogLevel:               order.LogLevel,
		DstBlobData: JobPartPlanDstBlob{
			BlobType:                         order.BlobAttributes.BlobType,
			NoGuessMimeType:                  order.BlobAttributes.NoGuessMimeType,
			ContentTypeLength:                uint16(len(order.BlobAttributes.ContentType)),
			ContentEncodingLength:            uint16(len(order.BlobAttributes.ContentEncoding)),
			ContentDispositionLength:         uint16(len(order.BlobAttributes.ContentDisposition)),
			ContentLanguageLength:            uint16(len(order.BlobAttributes.ContentLanguage)),
			CacheControlLength:               uint16(len(order.BlobAttributes.CacheControl)),
			PutMd5:                           order.BlobAttributes.PutMd5, // here because it relates to uploads (blob destination)
			BlockBlobTier:                    order.BlobAttributes.BlockBlobTier,
			PageBlobTier:                     order.BlobAttributes.PageBlobTier,
			MetadataLength:                   uint16(len(order.BlobAttributes.Metadata)),
			BlockSize:                        blockSize,
			PutBlobSize:                      putBlobSize,
			BlobTagsLength:                   uint16(len(order.BlobAttributes.BlobTagsString)),
			CpkInfo:                          order.CpkOptions.CpkInfo,
			CpkScopeInfoLength:               uint16(len(order.CpkOptions.CpkScopeInfo)),
			IsSourceEncrypted:                order.CpkOptions.IsSourceEncrypted,
			SetPropertiesFlags:               order.SetPropertiesFlags,
			DeleteDestinationFileIfNecessary: order.BlobAttributes.DeleteDestinationFileIfNecessary,
		},
		DstLocalData: JobPartPlanDstLocal{
			PreserveLastModifiedTime: order.BlobAttributes.PreserveLastModifiedTime,
			MD5VerificationOption:    order.BlobAttributes.MD5ValidationOption, // here because it relates to downloads (file destination)
		},
		PreservePermissions:     order.PreservePermissions,
		PreserveInfo:            order.PreserveInfo,
		PreservePOSIXProperties: order.PreservePOSIXProperties,
		// For S2S copy, per JobPartPlan info
		S2SGetPropertiesInBackend:      order.S2SGetPropertiesInBackend,
		S2SSourceChangeValidation:      order.S2SSourceChangeValidation,
		S2SInvalidMetadataHandleOption: order.S2SInvalidMetadataHandleOption,
		DestLengthValidation:           order.DestLengthValidation,
		BlobFSRecursiveDelete:          order.BlobFSRecursiveDelete,
		atomicJobStatus:                common.EJobStatus.InProgress(), // We default to InProgress
		DeleteSnapshotsOption:          order.BlobAttributes.DeleteSnapshotsOption,
		PermanentDeleteOption:          order.BlobAttributes.PermanentDeleteOption,
		RehydratePriority:              order.BlobAttributes.RehydratePriority,
		DstFileData: JobPartPlanDstFile{
			TrailingDot: order.FileAttributes.TrailingDot,
		},
		SymlinkHandling:  order.SymlinkHandlingType,
		JobPartType:      order.JobPartType,
		HardlinkHandling: order.HardlinkHandlingType,
	}

	// Copy any strings into their respective fields
	// do NOT copy Source/DestinationRoot.SAS, since we do NOT persist SASs
	copy(jpph.SourceRoot[:], order.SourceRoot.Value)
	copy(jpph.SourceExtraQuery[:], order.SourceRoot.ExtraQuery)
	copy(jpph.DestinationRoot[:], order.DestinationRoot.Value)
	copy(jpph.DestExtraQuery[:], order.DestinationRoot.ExtraQuery)
	copy(jpph.DstBlobData.ContentType[:], order.BlobAttributes.ContentType)
	copy(jpph.DstBlobData.ContentEncoding[:], order.BlobAttributes.ContentEncoding)
	copy(jpph.DstBlobData.ContentLanguage[:], order.BlobAttributes.ContentLanguage)
	copy(jpph.DstBlobData.ContentDisposition[:], order.BlobAttributes.ContentDisposition)
	copy(jpph.DstBlobData.CacheControl[:], order.BlobAttributes.CacheControl)
	copy(jpph.DstBlobData.Metadata[:], order.BlobAttributes.Metadata)
	copy(jpph.DstBlobData.BlobTags[:], order.BlobAttributes.BlobTagsString)
	copy(jpph.DstBlobData.CpkScopeInfo[:], order.CpkOptions.CpkScopeInfo)

	eof += writeValue(file, &jpph)

	// write the command string in the JobPart Plan file
	bytesWritten, err := file.WriteString(order.CommandString)
	if err != nil {
		panic(err)
	}
	eof += int64(bytesWritten)

	// ensure 8 byte alignment so that Atomic fields of JobPartPlanTransfer can actually be accessed atomically
	paddingLen := ((eof + 7) & ^7) - eof
	if paddingLen != 0 {
		bytesWritten, err := file.Write(make([]byte, paddingLen))
		if err != nil {
			panic(err)
		}
		eof += int64(bytesWritten)
	}

	// srcDstStringsOffset points to after the header & all the transfers; this is where the src/dst strings go for each transfer
	srcDstStringsOffset := make([]int64, jpph.NumTransfers)

	// Initialize the offset for the 1st transfer's src/dst strings
	currentSrcStringOffset := eof + int64(unsafe.Sizeof(JobPartPlanTransfer{}))*int64(jpph.NumTransfers)

	// Write each transfer to the Job Part Plan file (except for the src/dst strings; comes come later)
	for t := range order.Transfers.List {
		if len(order.Transfers.List[t].Source) > math.MaxInt16 || len(order.Transfers.List[t].Destination) > math.MaxInt16 {
			panic(fmt.Sprintf("The file %s exceeds azcopy's current maximum path length on either the source or the destination.", order.Transfers.List[t].Source))
		}

		// Prepare info for JobPartPlanTransfer
		// Sending Metadata type to Transfer could ensure strong type validation.
		// TODO: discuss the performance drop of marshaling metadata twice
		srcMetadataLength := 0
		if order.Transfers.List[t].Metadata != nil {
			metadataStr, err := order.Transfers.List[t].Metadata.Marshal()
			if err != nil {
				panic(err)
			}
			srcMetadataLength = len(metadataStr)
		}
		if srcMetadataLength > math.MaxInt16 {
			panic(fmt.Sprintf("The metadata on source file %s exceeds azcopy's current maximum metadata length, and cannot be processed.", order.Transfers.List[t].Source))
		}

		srcBlobTagsLength := 0
		if order.Transfers.List[t].BlobTags != nil {
			blobTagsStr := order.Transfers.List[t].BlobTags.ToString()
			srcBlobTagsLength = len(blobTagsStr)
		}
		if srcBlobTagsLength > math.MaxInt16 {
			panic(fmt.Sprintf("The length of tags %s exceeds maximum allowed length, and cannot be processed.", order.Transfers.List[t].BlobTags))
		}
		// Create & initialize this transfer's Job Part Plan Transfer
		jppt := JobPartPlanTransfer{
			SrcOffset:      currentSrcStringOffset, // SrcOffset of the src string
			SrcLength:      int16(len(order.Transfers.List[t].Source)),
			DstLength:      int16(len(order.Transfers.List[t].Destination)),
			EntityType:     order.Transfers.List[t].EntityType,
			ModifiedTime:   order.Transfers.List[t].LastModifiedTime.UnixNano(),
			SourceSize:     order.Transfers.List[t].SourceSize,
			CompletionTime: 0,
			// For S2S copy, per Transfer source's properties
			SrcContentTypeLength:        int16(len(order.Transfers.List[t].ContentType)),
			SrcContentEncodingLength:    int16(len(order.Transfers.List[t].ContentEncoding)),
			SrcContentLanguageLength:    int16(len(order.Transfers.List[t].ContentLanguage)),
			SrcContentDispositionLength: int16(len(order.Transfers.List[t].ContentDisposition)),
			SrcCacheControlLength:       int16(len(order.Transfers.List[t].CacheControl)),
			SrcContentMD5Length:         int16(len(order.Transfers.List[t].ContentMD5)),
			SrcMetadataLength:           int16(srcMetadataLength),
			SrcBlobTypeLength:           int16(len(order.Transfers.List[t].BlobType)),
			SrcBlobTierLength:           int16(len(order.Transfers.List[t].BlobTier)),
			SrcBlobVersionIDLength:      int16(len(order.Transfers.List[t].BlobVersionID)),
			SrcBlobSnapshotIDLength:     int16(len(order.Transfers.List[t].BlobSnapshotID)),
			SrcBlobTagsLength:           int16(srcBlobTagsLength),

			atomicTransferStatus: common.ETransferStatus.Started(), // Default
			// ChunkNum:                getNumChunks(uint64(order.Transfers.List[t].SourceSize), uint64(data.BlockSize)),
			TargetHardlinkFilePathLength: int16(len(order.Transfers.List[t].TargetHardlinkFile)),
		}
		eof += writeValue(file, &jppt) // Write the transfer entry

		// The NEXT transfer's src/dst string come after THIS transfer's src/dst strings
		srcDstStringsOffset[t] = currentSrcStringOffset

		currentSrcStringOffset += int64(jppt.SrcLength + jppt.DstLength + jppt.SrcContentTypeLength +
			jppt.SrcContentEncodingLength + jppt.SrcContentLanguageLength + jppt.SrcContentDispositionLength +
			jppt.SrcCacheControlLength + jppt.SrcContentMD5Length + jppt.SrcMetadataLength +
			jppt.SrcBlobTypeLength + jppt.SrcBlobTierLength + jppt.SrcBlobVersionIDLength + jppt.SrcBlobSnapshotIDLength + jppt.SrcBlobTagsLength + jppt.TargetHardlinkFilePathLength)
	}

	// All the transfers were written; now write each transfer's src/dst strings
	for t := range order.Transfers.List {
		// Sanity check: Verify that we are were we think we are and that no bug has occurred
		if eof != srcDstStringsOffset[t] {
			panic(errors.New("job plan file's EOF and the transfer's offset didn't line up; filename: " + order.Transfers.List[t].Source))
		}

		// Write the src & dst strings to the job part plan file
		bytesWritten, err := file.WriteString(order.Transfers.List[t].Source)
		common.PanicIfErr(err)
		eof += int64(bytesWritten)
		// write the destination string in memory map file
		bytesWritten, err = file.WriteString(order.Transfers.List[t].Destination)
		common.PanicIfErr(err)
		eof += int64(bytesWritten)

		// For S2S copy (and, in the case of Content-MD5, always), write the src properties
		if len(order.Transfers.List[t].ContentType) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].ContentType)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].ContentEncoding) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].ContentEncoding)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].ContentLanguage) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].ContentLanguage)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].ContentDisposition) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].ContentDisposition)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].CacheControl) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].CacheControl)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if order.Transfers.List[t].ContentMD5 != nil { // if non-nil but 0 len, will simply not be read by the consumer (since length is zero)
			bytesWritten, err = file.WriteString(string(order.Transfers.List[t].ContentMD5))
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		// For S2S copy, write the src metadata
		if order.Transfers.List[t].Metadata != nil {
			metadataStr, err := order.Transfers.List[t].Metadata.Marshal()
			common.PanicIfErr(err)

			bytesWritten, err = file.WriteString(metadataStr)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].BlobType) != 0 {
			bytesWritten, err = file.WriteString(string(order.Transfers.List[t].BlobType))
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].BlobTier) != 0 {
			bytesWritten, err = file.WriteString(string(order.Transfers.List[t].BlobTier))
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].BlobVersionID) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].BlobVersionID)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].BlobSnapshotID) != 0 {
			bytesWritten, err = file.WriteString(order.Transfers.List[t].BlobSnapshotID)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		// For S2S copy, write the source tags in job part plan transfer
		if len(order.Transfers.List[t].BlobTags) != 0 {
			blobTagsStr := order.Transfers.List[t].BlobTags.ToString()
			bytesWritten, err = file.WriteString(blobTagsStr)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
		if len(order.Transfers.List[t].TargetHardlinkFile) != 0 {
			TargetHardlinkFileStr := order.Transfers.List[t].TargetHardlinkFile
			bytesWritten, err = file.WriteString(TargetHardlinkFileStr)
			common.PanicIfErr(err)
			eof += int64(bytesWritten)
		}
	}
	// the file is closed to due to defer above
}
