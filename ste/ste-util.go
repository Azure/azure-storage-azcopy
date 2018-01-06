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
)

func parseStringToJobInfo(s string) (jobId common.JobID, partNo common.PartNumber){
	//todo : use scanf
	return
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
	files := listFileWithExtension(".STE")
	for index := 0; index < len(files) ; index++{
		fileName := files[index].Name()
		jobHandler := new(JobPartPlanInfo)
		err := jobHandler.initialize(steContext, fileName)
		if err != nil{
			return err
		}
		jobIdString, partNumber := parseStringToJobInfo(fileName)
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