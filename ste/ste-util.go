package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"os"
	"path/filepath"
	"regexp"
	"fmt"
	"encoding/hex"
	"io"
	"reflect"
	"encoding/binary"
	"encoding/base64"
	"strings"
	"strconv"
	"errors"
)

// parseStringToJobInfo api parses the file name to extract the job Id, part number and schema version number
// Returns the JobId, PartNumber and data schema version
func parseStringToJobInfo(s string) (jobId common.JobID, partNo common.PartNumber, version common.Version){

	/*
		* Split string using delimeter '-'
		* First part of string is JobId
		* Other half of string contains part number and version number separated by '.'
		* split other half using '.' as delimeter
	    * first half of this split is part number while the other half is version number with stev as prefix
	    * remove the stev prefix from version number
	    * parse part number string and version number string into uint32 integer
	 */
	// split the s string using '-' which separates the jobId from the rest of string in filename
	parts := strings.Split(s, "-")
	jobIdString := parts[0]
	partNo_versionNo := parts[1]

	// after jobId string, partNo and schema version are separated using '.'
	parts = strings.Split(partNo_versionNo, ".")
	partNoString := parts[0]

	// removing 'stev' prefix from version number
	versionString := parts[1][4:]

	// parsing part number string into uint32 integer
	partNo64, err := strconv.ParseUint(partNoString, 10, 32)
	if err != nil{
		errMsg := fmt.Sprintf("error parsing the mememory map file name %s", s)
		panic(errors.New(errMsg))
	}

	// parsing version number string into uint32 integer
	versionNo64, err := strconv.ParseUint(versionString, 10, 32)
	if err != nil{
		errMsg := fmt.Sprintf("error parsing the mememory map file name %s", s)
		panic(errors.New(errMsg))
	}
	fmt.Println(" string ", common.JobID(jobIdString), " ", common.PartNumber(partNo64), " ", common.Version(versionNo64))
	return common.JobID(jobIdString), common.PartNumber(partNo64), common.Version(versionNo64)
}

func formatJobInfoToString(jobPartOrder common.CopyJobPartOrder) (string){
	versionIdString := fmt.Sprintf("%05d", jobPartOrder.Version)
	partNoString := fmt.Sprintf("%05d", jobPartOrder.PartNum)
	fileName := string(jobPartOrder.ID) + "-" + partNoString + ".stev" + versionIdString
	return fileName
}


func convertJobIdToByteFormat(jobIDString common.JobID) ([128 / 8]byte){
	var jobID [128 /8] byte
	guIdBytes, err := hex.DecodeString(string(jobIDString))
	if err != nil {
		panic(err)
	}
	copy(jobID[:], guIdBytes)
	return jobID
}

// writeInterfaceDataToWriter api writes the content of given interface to the io writer
func writeInterfaceDataToWriter( writer io.Writer, f interface{}, structSize uint64) (int, error){

	// bytesWritten keeps the track of number of actual bytes written to the writer
	var bytesWritten uint64 = 0

	// nextOffset determines the next offset at which the next element should be written
	var nextOffset uint64= 0

	// currentOffset keeps the track of end offset till which have been written to the writer
	var currentOffset uint64 = 0
	var padBytes [8]byte
	// get the num of elements in interface
	var elements = reflect.ValueOf(f).Elem()
	for val := 0; val < elements.NumField(); val++{
		//get the alignment of type of element
		align := elements.Type().Field(val).Type.FieldAlign()

		//rounding up the current offset to next multiple of alignment
		nextOffset = roundUp(currentOffset, uint64(align))

		/*adding 0's for the difference of number of bytes between current offset and
		next offset to align the element*/
		err := binary.Write(writer, binary.LittleEndian, padBytes[0: (nextOffset - currentOffset)])
		if err != nil {
			return 0, err
		}

		bytesWritten += uint64(nextOffset - currentOffset)
		valueOfField := elements.Field(val)
		elementValue := reflect.ValueOf(valueOfField.Interface()).Interface()
		sizeElementValue := uint64(valueOfField.Type().Size())

		// writing the element value to the writer
		err = binary.Write(writer, binary.LittleEndian, elementValue)
		if err != nil {
			return 0, err
		}
		bytesWritten += sizeElementValue
		currentOffset = bytesWritten
	}

	err := binary.Write(writer, binary.LittleEndian, padBytes[0: (structSize - bytesWritten)])
	if err != nil {
		return 0, err
	}
	bytesWritten += (structSize - bytesWritten)
	return int(bytesWritten), nil
}

func convertJobIdBytesToString(jobId [128 /8]byte) (string){
	jobIdString := fmt.Sprintf("%x%x%x%x%x", jobId[0:4], jobId[4:6], jobId[6:8], jobId[8:10], jobId[10:])
	return jobIdString
}

func reconstructTheExistingJobPart() (error){
	files := listFileWithExtension(".stev")
	for index := 0; index < len(files) ; index++{
		fileName := files[index].Name()
		jobIdString, partNumber, versionNumber := parseStringToJobInfo(fileName)
		if versionNumber != dataSchemaVersion{
			continue
		}
		jobHandler := new(JobPartPlanInfo)
		err := jobHandler.initialize(steContext, fileName)
		if err != nil{
			return err
		}
		//fmt.Println("jobID & partno ", jobIdString, " ", partNumber, " ", versionNumber)
		putJobPartInfoHandlerIntoMap(jobHandler, jobIdString, partNumber, &JobPartInfoMap)
	}
	return nil
}


func roundUp(numToRound uint64, multipleOf uint64) (uint64){
	if multipleOf <= 1{
		return numToRound
	}
	if numToRound == 0 {
		return 0
	}
	remainder := numToRound % multipleOf
	if remainder == 0{
		return numToRound;
	}
	return numToRound + multipleOf - remainder
}

func listFileWithExtension(ext string) []os.FileInfo {
	pathS, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	var files []os.FileInfo
	filepath.Walk(pathS, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			r, err := regexp.MatchString(ext, f.Name())
			if err == nil && r {
				files = append(files, f)
			}
		}
		return nil
	})
	return files
}

func creatingTheBlockIds(numBlocks int) ([] string){
	blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }

	blockIDIntToBase64 := func(blockID int) string {
		binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
		binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
		return blockIDBinaryToBase64(binaryBlockID)
	}

	base64BlockIDs := make([]string, numBlocks)

	for index := 0; index < numBlocks ; index++ {
		base64BlockIDs[index] = blockIDIntToBase64(index)
	}
	return base64BlockIDs
}

func getTransferMsgDetail (jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32) (TransferMsgDetail){
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, &JobPartInfoMap)
	if err != nil{
		panic(err)
	}
	jPartPlanPointer := jHandler.getJobPartPlanPointer()
	sourceType := jPartPlanPointer.SrcLocationType
	destinationType := jPartPlanPointer.DstLocationType
	source, destination := jHandler.getTransferSrcDstDetail(transferEntryIndex)
	chunkSize := jPartPlanPointer.BlobData.BlockSizeInKB
	return TransferMsgDetail{jobId, partNo,transferEntryIndex, chunkSize, sourceType,
		source, destinationType, destination, jHandler.TrasnferInfo[transferEntryIndex].ctx, jHandler.TrasnferInfo[transferEntryIndex].cancel}
}

func updateChunkInfo(jobId common.JobID, partNo common.PartNumber, transferEntryIndex uint32, chunkIndex uint16, status uint8) {
	jHandler, err := getJobPartInfoHandlerFromMap(jobId, partNo, &JobPartInfoMap)
	if err != nil{
		panic(err)
	}
	jHandler.updateTheChunkInfo(transferEntryIndex, chunkIndex, [128 /8]byte{}, status)
}