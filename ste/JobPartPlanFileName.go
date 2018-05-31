package ste

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type JobPartPlanFileName string

func (jppfn *JobPartPlanFileName) GetJobPartPlanPath() string {
	return fmt.Sprintf("%s%s%s", JobsAdmin.AppPathFolder(), string(os.PathSeparator), string(*jppfn))
}

const jobPartPlanFileNameFormat = "%v--%05d.steV%d"

// TODO: This needs testing
func (jpfn JobPartPlanFileName) Parse() (jobID common.JobID, partNumber common.PartNumber, err error) {
	var dataSchemaVersion common.Version
	//n, err := fmt.Sscanf(string(jpfn), jobPartPlanFileNameFormat, &jobID, &partNumber, &dataSchemaVersion)
	//if err != nil || n != 3 {
	//	panic(err)
	//}
	//if dataSchemaVersion != DataSchemaVersion {
	//	err = fmt.Errorf("job part Plan file's data schema version ('%d') doesn't match whatthis app requires ('%d')", dataSchemaVersion, DataSchemaVersion)
	//}
	//TODO: confirm the alternative approach. fmt.Sscanf not working for reading back string into struct JobId.
	jpfnSplit := strings.Split(string(jpfn), "--")
	jobId, err := common.ParseJobID(jpfnSplit[0])
	if err != nil {
		err = fmt.Errorf("failed to parse the JobId from JobPartFileName %s. Failed with error %s", string(jpfn), err.Error())
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

func (jpfn JobPartPlanFileName) Map() JobPartPlanMMF {
	// opening the file with given filename
	file, err := os.OpenFile(jpfn.GetJobPartPlanPath(), os.O_RDWR, 0644) // TODO: Check this permission
	if err != nil {
		panic(err)
	}
	// Ensure the file gets closed (although we can continue to use the MMF)
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	mmf, err := common.NewMMF(file, true, 0, fileInfo.Size())
	if err != nil {
		panic(err)
	}
	return JobPartPlanMMF(mmf)
}

// createJobPartPlanFile creates the memory map JobPartPlanHeader using the given JobPartOrder and JobPartPlanBlobData
func (jpfn JobPartPlanFileName) Create(order common.CopyJobPartOrderRequest) {
	// Validate that the passed-in strings can fit in their respective fields
	if len(order.BlobAttributes.ContentType) > len(JobPartPlanDstBlob{}.ContentType) {
		panic(fmt.Errorf("content type string it too large: %q", order.BlobAttributes.ContentType))
	}
	if len(order.BlobAttributes.ContentEncoding) > len(JobPartPlanDstBlob{}.ContentEncoding) {
		panic(fmt.Errorf("content encoding string it too large: %q", order.BlobAttributes.ContentEncoding))
	}
	if len(order.BlobAttributes.Metadata) > len(JobPartPlanDstBlob{}.Metadata) {
		panic(fmt.Errorf("metadata string it too large: %q", order.BlobAttributes.Metadata))
	}

	// This nested function writes a structure value to an io.Writer & returns the number of bytes written
	writeValue := func(writer io.Writer, v interface{}) int64 {
		rv := reflect.ValueOf(v)
		structSize := reflect.TypeOf(v).Elem().Size()
		slice := reflect.SliceHeader{Data: rv.Pointer(), Len: int(structSize), Cap: int(structSize)}
		byteSlice := *(*[]byte)(unsafe.Pointer(&slice))
		err := binary.Write(writer, binary.LittleEndian, byteSlice)
		if err != nil {
			panic(err)
		}
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
	//planPathname := planDir + "/" + string(jpfn)
	file, err := os.Create(jpfn.GetJobPartPlanPath())
	if err != nil {
		panic(fmt.Errorf("couldn't create job part plan file %q: %v", jpfn, err))
	}
	defer file.Close()

	// if block size from the front-end is set to 0, block size is set to default block size
	blockSize := order.BlobAttributes.BlockSizeInBytes
	if blockSize == 0 { // TODO: Fix below
		blockSize = common.DefaultBlockBlobBlockSize
		/*switch order.BlobAttributes.BlobType {
		case common.BlobType{}.Block():
			blockSize = common.DefaultBlockBlobBlockSize
		case common.BlobType{}.Append():
			blockSize = common.DefaultAppendBlobBlockSize
		case common.BlobType{}.Page():
			blockSize = common.DefaultPageBlobChunkSize
		default:
			panic(errors.New("unrecognized blob type"))
		}*/
	}
	// Initialize the Job Part's Plan header
	jpph := JobPartPlanHeader{
		Version:            DataSchemaVersion,
		JobID:              order.JobID,
		PartNum:            order.PartNum,
		IsFinalPart:        order.IsFinalPart,
		ForceWrite:         order.ForceWrite,
		Priority:           order.Priority,
		TTLAfterCompletion: uint32(time.Time{}.Nanosecond()),
		FromTo:             order.FromTo,
		NumTransfers:       uint32(len(order.Transfers)),
		LogLevel:           order.LogLevel,
		DstBlobData: JobPartPlanDstBlob{
			//BlobType:              order.OptionalAttributes.BlobType,
			NoGuessMimeType:       order.BlobAttributes.NoGuessMimeType,
			ContentTypeLength:     uint16(len(order.BlobAttributes.ContentType)),
			ContentEncodingLength: uint16(len(order.BlobAttributes.ContentEncoding)),
			BlockBlobTier:         order.BlobAttributes.BlockBlobTier,
			PageBlobTier:          order.BlobAttributes.PageBlobTier,
			MetadataLength:        uint16(len(order.BlobAttributes.Metadata)),
			BlockSize:             blockSize,
		},
		DstLocalData: JobPartPlanDstLocal{
			PreserveLastModifiedTime: order.BlobAttributes.PreserveLastModifiedTime,
		},
		atomicJobStatus: common.EJobStatus.InProgress(), // We default to InProgress
	}
	// Copy any strings into their respective fields
	copy(jpph.DstBlobData.ContentType[:], order.BlobAttributes.ContentType)
	copy(jpph.DstBlobData.ContentEncoding[:], order.BlobAttributes.ContentEncoding)
	copy(jpph.DstBlobData.Metadata[:], order.BlobAttributes.Metadata)
	fmt.Println("")
	eof += writeValue(file, &jpph)

	// srcDstStringsOffset points to after the header & all the transfers; this is where the src/dst strings go for each transfer
	srcDstStringsOffset := make([]int64, jpph.NumTransfers)

	// Initialize the offset for the 1st transfer's src/dst strings
	currentSrcStringOffset := eof + int64(unsafe.Sizeof(JobPartPlanTransfer{}))*int64(jpph.NumTransfers)

	// Write each transfer to the Job Part Plan file (except for the src/dst strings; comes come later)
	for t := range order.Transfers {
		// Create & initialize this transfer's Job Part Plan Transfer
		jppt := JobPartPlanTransfer{
			SrcOffset:            currentSrcStringOffset, // SrcOffset of the src string
			SrcLength:            int16(len(order.Transfers[t].Source)),
			DstLength:            int16(len(order.Transfers[t].Destination)),
			ModifiedTime:         order.Transfers[t].LastModifiedTime.UnixNano(),
			SourceSize:           order.Transfers[t].SourceSize,
			CompletionTime:       0,
			atomicTransferStatus: common.ETransferStatus.NotStarted(), // Default
			//ChunkNum:                getNumChunks(uint64(order.Transfers[t].SourceSize), uint64(data.BlockSize)),
		}
		eof += writeValue(file, &jppt) // Write the transfer entry

		// The NEXT transfer's src/dst string come after THIS transfer's src/dst strings
		srcDstStringsOffset[t] = currentSrcStringOffset

		currentSrcStringOffset += int64(jppt.SrcLength + jppt.DstLength)
	}

	// All the transfers were written; now write each each transfer's src/dst strings
	for t := range order.Transfers {
		// Sanity check: Verify that we are were we think we are and that no bug has occurred
		if eof != srcDstStringsOffset[t] {
			panic(errors.New("error writing src/dst strings to job part plan file"))
		}

		// Write the src & dst strings to the job part plan file
		bytesWritten, err := file.WriteString(order.Transfers[t].Source)
		if err != nil {
			panic(err)
		}
		// write the destination string in memory map file
		eof += int64(bytesWritten)
		bytesWritten, err = file.WriteString(order.Transfers[t].Destination)
		if err != nil {
			panic(err)
		}
		eof += int64(bytesWritten)
	}
	// the file is closed to due to defer above
}
