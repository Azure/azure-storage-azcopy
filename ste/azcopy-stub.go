package ste

import (
	"fmt"
	"flag"
	"os"
	"net/http"
	"encoding/json"
	"bytes"
	"io/ioutil"
	"time"
	"io"
	"crypto/rand"
	"os/exec"
	"github.com/Azure/azure-storage-azcopy/common"
	"strconv"
)


func fetchJobStatus(jobId string) (error){
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil{
		return err
	}

	q := req.URL.Query()
	q.Add("type", "JobStatus")
	q.Add("GUID", jobId)
	q.Add("CheckpointTime", strconv.FormatUint(uint64(time.Now().Nanosecond()), 10))
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil{
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("request failed with status ", resp.Status)
	}

	defer resp.Body.Close()
	body, err:= ioutil.ReadAll(resp.Body)
	if err != nil{
		return err
	}
	var summary common.JobProgressSummary
	json.Unmarshal(body, &summary)
	fmt.Println("JobId ", jobId)
	fmt.Println("", summary.TotalNumberOfTransfer)
	fmt.Println("", summary.TotalNumberofTransferCompleted)
	fmt.Println("", summary.CompleteJobOrdered)
	fmt.Println("", summary.NumberOfTransferCompletedafterCheckpoint)
	fmt.Println("", summary.NumberOfTransferFailedAfterCheckpoint)
	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s status: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst, summary.FailedTransfers[index])
		fmt.Println(message)
	}
	return nil
}

func fetchJobPartStatus(jobId string , partNo string) (error){
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil{
		return err
	}

	q := req.URL.Query()
	q.Add("type", "PartStatus")
	q.Add("GUID", jobId)
	q.Add("Part", partNo)
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil{
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("request failed with status ", resp.Status)
	}

	defer resp.Body.Close()
	body, err:= ioutil.ReadAll(resp.Body)
	if err != nil{
		return err
	}
	var status common.TransfersStatus
	json.Unmarshal(body, &status)
	for index := 0; index < len(status.Status); index++{
		message := fmt.Sprintf("Source %s  Destination %s Status %d", status.Status[index].Src, status.Status[index].Dst, status.Status[index].Status)
		fmt.Println(message)
	}
	return nil
}

func sendUploadRequestToSTE(sourceFileName string, targetfileName string) {
	guId,err := newUUID()
	if err != nil {
		panic(err)
	}
	fmt.Println("Sending Upload Request TO STE")
	url := "http://localhost:1337"
	payload := common.JobPartToBlockBlob{
		common.CopyJobPartOrder{1,
			common.JobID(guId),
			0,
			false,
			HighJobPriority,
			common.Local,
			common.Blob,
			[]common.CopyTransfer{
				common.CopyTransfer{
					sourceFileName,
					targetfileName,
					time.Now(), 10},
					common.CopyTransfer{
						sourceFileName,
						targetfileName,
						time.Now(), 10}}},
		common.BlobData{"","", "", false, false, 1000}}

	payloadData, err := json.MarshalIndent(payload, "", "")
	fmt.Println("Marshalled Data ", string(payloadData))
	res, err := http.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payloadData))
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println("Response to request", res.Status, " ", body)
	time.Sleep(5 * time.Second)
	fetchJobPartStatus(guId, "0")
}

func sendStatusRequestToSTE(guid string, partNo string, transferIndex uint32, chunkIndex uint16){
	guId,err := newUUID()
	if err != nil {
		panic(err)
	}
	fmt.Println("Sending Upload Request TO STE")
	client := &http.Client{}
	url := "http://localhost:1337"
	payload := statusQuery{Guid:guId, PartNo:partNo, TransferIndex:transferIndex, ChunkIndex:chunkIndex}
	payloadData, err := json.MarshalIndent(payload, "", "")
	if err != nil {
		panic(err)
	}
	fmt.Println("Marshalled Data ", string(payloadData))
	req, err := http.NewRequest("PUT", url, bytes.NewReader(payloadData))
	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println("Response to request", res.Status, " ", body)
}

func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	code,err := fmt.Sprintf("%x%x%x%x%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
	fmt.Println(" Code", uuid)
	return code, err
}


func main(){
	fmt.Println("Welcome to the version of Project 0.0")

	var commandGiven = ""
	printCommand := flag.NewFlagSet("print" , flag.ExitOnError)
	sourceFileName := printCommand.String("src", "", "File Name to upload")
	targetFileName := printCommand.String("dst", "", "File Name to be copied to")

	statusCommand := flag.NewFlagSet("status", flag.ExitOnError)
	guid := statusCommand.String("guid", "", "")
	partNo := statusCommand.String("part", "", "")

	if len(os.Args) < 1 {
		fmt.Println("No Command Provided")
		os.Exit(1)
	}
	if len(os.Args) == 1{
		commandGiven = "normal"
	}else{
		commandGiven = os.Args[1]
	}

	switch commandGiven {
	case "normal":
		cmd := exec.Command("./AZCopy.exe", "StartSTE")
		err := cmd.Start()
		if err != nil{
			panic(err)
			os.Exit(1)
		}
	case "debug":
		go func(){
			//InitTransferEngine()
		}()
	case "print":
		printCommand.Parse(os.Args[2:])
		if printCommand.Parsed(){
			if *targetFileName == "" {
				printCommand.PrintDefaults()
				os.Exit(1)
			}
			if *sourceFileName == "" {
				printCommand.PrintDefaults()
				os.Exit(1)
			}
		}
		sendUploadRequestToSTE(*sourceFileName, *targetFileName)
	case "status":
		statusCommand.Parse(os.Args[2:])
		if statusCommand.Parsed(){
			if *guid == "" {
				printCommand.PrintDefaults()
				os.Exit(1)
			}
			if *partNo == "" {
				fetchJobStatus(*guid)
			}else {
				fetchJobPartStatus(*guid, *partNo)
			}
		}

	case "StartSTE":
		//InitTransferEngine()

	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}
