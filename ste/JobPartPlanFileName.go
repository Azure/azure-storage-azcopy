package ste

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io"
	"os"
	"reflect"
	"time"
	"unsafe"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
// checkCancelledJobsInJobMap api checks the JobPartPlan header of part 0 of each job
// JobPartPlan header of part 0 of each job determines the actual status of each job
// if the job status is cancelled, then it cleans up the job
func checkCancelledJobsInJobMap() {
	for jobID := range JobsAdmin.JobIDs() {
		// getting the jobInfo for part 0 of current jobId
		// since the status of Job is determined by the job status in JobPartPlan header of part 0
		jm, found := JobsAdmin.JobMgr(jobID)//.JobInfo(jobIds[index])
		if !found { continue }
		jpm, found := jm.JobPartMgr(0)
		if !found { continue }
		if jpm.Plan().atomicJobStatus == common.EJobStatus.Cancelled() {	// TODO: fix atomic to make thread safe
			jobsInfoMap.cleanUpJob(jobIds[index])
		}
		// if the jobstatus in JobPartPlan header of part 0 is cancelled and cleanup wasn't successful
		// if the part 0 was deleted successfully but other parts deletion wasn't successful
		// cleaning up the job now
		if jobInfo == nil || jobInfo.JobPartPlanInfo(0).getJobPartPlanPointer().Status() == JobCancelled {
		}
	}
}
*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type JobPartPlanFileName string

const jobPartPlanFileNameFormat = "%v--%05d.steV%d"

var planDir = ""	// TODO: Fix

// TODO: This needs testing
func (jpfn JobPartPlanFileName) Parse() (jobID common.JobID, partNumber common.PartNumber, err error) {
	var dataSchemaVersion common.Version
	n, err := fmt.Sscanf(string(jpfn), jobPartPlanFileNameFormat, &jobID, &partNumber, &dataSchemaVersion)
	if err != nil || n != 3 {
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
	file, err := os.OpenFile(string(jpfn), os.O_RDWR, 0644) // TODO: Check this permission
	if err != nil {
		panic(err)
	}
	// Ensure the file gets closed (although we can continue to use the MMF)
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	mmf, err := common.NewMMF(file, false, 0, fileInfo.Size())
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
		structSize := reflect.TypeOf(v).Len()
		slice := reflect.SliceHeader{Data: rv.Pointer(), Len: structSize, Cap: structSize}
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
	planPathname := planDir + "/" + string(jpfn)
	file, err := os.Create(planPathname)
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

		// Initialize the Job Part's Plan header
		jpph := JobPartPlanHeader{
			Version:            DataSchemaVersion,
			JobID:              order.JobID,
			PartNum:            order.PartNum,
			IsFinalPart:        order.IsFinalPart,
			Priority:           order.Priority,
			TTLAfterCompletion: uint32(time.Time{}.Nanosecond()),
			FromTo:        order.FromTo,
			NumTransfers:       uint32(len(order.Transfers)),
			LogLevel:           order.LogLevel,
			DstBlobData: JobPartPlanDstBlob{
				//BlobType:              order.OptionalAttributes.BlobType,
				NoGuessMimeType:       order.BlobAttributes.NoGuessMimeType,
				ContentTypeLength:     uint16(len(order.BlobAttributes.ContentType)),
				ContentEncodingLength: uint16(len(order.BlobAttributes.ContentEncoding)),
				MetadataLength:        uint16(len(order.BlobAttributes.Metadata)),
				BlockSize:             blockSize,
			},
			DstLocalData: JobPartPlanDstLocal{
				PreserveLastModifiedTime: order.BlobAttributes.PreserveLastModifiedTime,
			},
			atomicJobStatus: common.JobStatus{}.InProgress(), // We default to InProgress
		}
		// Copy any strings into their respective fields
		copy(jpph.DstBlobData.ContentType[:], order.BlobAttributes.ContentType)
		copy(jpph.DstBlobData.ContentEncoding[:], order.BlobAttributes.ContentEncoding)
		copy(jpph.DstBlobData.Metadata[:], order.BlobAttributes.Metadata)
		eof += writeValue(file, &jpph)

		// srcDstStringsOffset points to after the header & all the transfers; this is where the src/dst strings go for each transfer
		srcDstStringsOffset := make([]int64, jpph.NumTransfers)

		// Initialize the offset for the 1st transfer's src/dst strings
		srcDstStringsOffset[0] = eof + int64(unsafe.Sizeof(JobPartPlanTransfer{}))*int64(jpph.NumTransfers)

		// Write each transfer to the Job Part Plan file (except for the src/dst strings; comes come later)
		for t := range order.Transfers {
			// Create & initialize this transfer's Job Part Plan Transfer
			jppt := JobPartPlanTransfer{
				Offset:               srcDstStringsOffset[t], // Offset of the src string
				SrcLength:            int16(len(order.Transfers[t].Source)),
				DstLength:            int16(len(order.Transfers[t].Destination)),
				ModifiedTime:         uint32(order.Transfers[t].LastModifiedTime.UnixNano()),
				SourceSize:           order.Transfers[t].SourceSize,
				CompletionTime:       0,
				atomicTransferStatus: common.ETransferStatus.NotStarted(), // Default
				//ChunkNum:                getNumChunks(uint64(order.Transfers[t].SourceSize), uint64(data.BlockSize)),
			}
			eof += writeValue(file, &jppt) // Write the transfer entry

			// The NEXT transfer's src/dst string come after THIS transfer's src/dst strings
			srcDstStringsOffset[t+1] = srcDstStringsOffset[t] + int64(jppt.SrcLength+jppt.DstLength)
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
}
