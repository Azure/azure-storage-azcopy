package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"strings"
	"math"
	"os"
	"path/filepath"
	"regexp"
)

func parseStringToJobInfo(s string) (jobId common.JobID, partNo common.PartNumber){
	//todo : use scanf
	return
}

func formatJobInfoToString(jobId common.JobID, partNo common.PartNumber) (string){
	//todo : use sprintf
	return ""
}

func reconstructTheExistingJobPart() (error){
	files := listFileWithExtension(".STE")
	for index := 0; index < len(files) ; index++{
		fileName := files[index].Name()

		//todo : create a sep func to return job id and parts from file name
		fileName = strings.Split(fileName, ".")[0]
		fileNameParts := strings.Split(fileName, "-")
		jobIdString := fileNameParts[0]
		partNoString := fileNameParts[1]
		jobHandler := new(JobPartPlanInfo)
		err := jobHandler.initialize(steContext,
			JobPart{ jobIdString, partNoString, math.MaxUint32,math.MaxUint16,
				false, math.MaxUint16, nil}, JobPartPlanBlobData{},
			true)
		if err != nil{
			return err
		}
		putJobPartInfoHandlerIntoMap(jobHandler, jobIdString, partNoString, &JobPartInfoMap)
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