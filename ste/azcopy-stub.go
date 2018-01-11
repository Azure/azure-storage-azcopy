package ste
//
//import (
//	"fmt"
//	"net/http"
//	"encoding/json"
//	"bytes"
//	"io/ioutil"
//	"time"
//	"io"
//	"crypto/rand"
//	"github.com/Azure/azure-storage-azcopy/common"
//	"strconv"
//	"os"
//	"flag"
//	"os/exec"
//)
//
//
//func listActiveJobs() (error){
//	url := "http://localhost:1337"
//	client := &http.Client{}
//	req, err := http.NewRequest("GET", url, nil)
//	if err != nil{
//		return err
//	}
//
//	q := req.URL.Query()
//	q.Add("type", "JobListing")
//	req.URL.RawQuery = q.Encode()
//
//	resp, err := client.Do(req)
//	if err != nil{
//		return err
//	}
//	if resp.StatusCode != http.StatusAccepted {
//		fmt.Println("request failed with status ", resp.Status)
//	}
//
//	defer resp.Body.Close()
//	body, err:= ioutil.ReadAll(resp.Body)
//	if err != nil{
//		return err
//	}
//	var jobList common.ExistingJobDetails
//	json.Unmarshal(body, &jobList)
//	fmt.Println("Existing Jobs")
//	for index := 0; index < len(jobList.JobIds); index++ {
//		message := fmt.Sprintf("JobId %s", jobList.JobIds[index])
//		fmt.Println(message)
//	}
//	return nil
//}
//
//func getJobDetails(jobId string) (error){
//	url := "http://localhost:1337"
//	client := &http.Client{}
//	req, err := http.NewRequest("GET", url, nil)
//	if err != nil{
//		return err
//	}
//
//	q := req.URL.Query()
//	q.Add("type", "JobDetail")
//	q.Add("GUID", jobId)
//	req.URL.RawQuery = q.Encode()
//
//	resp, err := client.Do(req)
//	if err != nil{
//		return err
//	}
//	if resp.StatusCode != http.StatusAccepted {
//		fmt.Println("request failed with status ", resp.Status)
//	}
//
//	defer resp.Body.Close()
//	body, err:= ioutil.ReadAll(resp.Body)
//	if err != nil{
//		return err
//	}
//	var jobDetail common.JobOrderDetails
//	json.Unmarshal(body, &jobDetail)
//	fmt.Println("Existing Jobs")
//	for index := 0; index < len(jobDetail.PartsDetail); index++ {
//		message := fmt.Sprintf("---------- Details for JobId %s and Part Number %d ---------------", jobId, jobDetail.PartsDetail[index].PartNum)
//		fmt.Println(message)
//		for tIndex := 0; tIndex < len(jobDetail.PartsDetail[index].TransferDetails); tIndex++{
//			message := fmt.Sprintf("--- transfer Id - %d; Source - %s; Destination - %s", index, jobDetail.PartsDetail[index].TransferDetails[tIndex].Src,
//																											jobDetail.PartsDetail[index].TransferDetails[tIndex].Dst)
//			fmt.Println(message)
//		}
//	}
//	return nil
//}
//
//func fetchJobStatus(jobId string) (error){
//	url := "http://localhost:1337"
//	client := &http.Client{}
//	req, err := http.NewRequest("GET", url, nil)
//	if err != nil{
//		return err
//	}
//	checkPointTime := time.Now()
//	q := req.URL.Query()
//	q.Add("type", "JobStatus")
//	q.Add("GUID", jobId)
//	q.Add("CheckpointTime", strconv.FormatUint(uint64(checkPointTime.Nanosecond()), 10))
//	req.URL.RawQuery = q.Encode()
//
//	resp, err := client.Do(req)
//	if err != nil{
//		return err
//	}
//	if resp.StatusCode != http.StatusAccepted {
//		fmt.Println("request failed with status ", resp.Status)
//	}
//
//	defer resp.Body.Close()
//	body, err:= ioutil.ReadAll(resp.Body)
//	if err != nil{
//		return err
//	}
//	var summary common.JobProgressSummary
//	json.Unmarshal(body, &summary)
//	fmt.Println("-----------------Progress Summary for JobId ", jobId," ------------------")
//	fmt.Println("Total Number of Transfer ", summary.TotalNumberOfTransfer)
//	fmt.Println("Total Number of Transfer Completed ", summary.TotalNumberofTransferCompleted)
//	fmt.Println("Total Number of Transfer Failed ", summary.TotalNumberofFailedTransfer)
//	fmt.Println("Has the final part been ordered ", summary.CompleteJobOrdered)
//	fmt.Println("Last CheckPoint Time ", checkPointTime)
//	fmt.Println("Number of Transfer Completed After CheckPoint", summary.NumberOfTransferCompletedafterCheckpoint)
//	fmt.Println("Number of Transfer Failed After CheckPoint", summary.NumberOfTransferFailedAfterCheckpoint)
//	fmt.Println("Progress of Job in terms of Perecentage ", summary.PercentageProgress)
//	for index := 0; index < len(summary.FailedTransfers); index++ {
//		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s status: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
//		fmt.Println(message)
//	}
//	return nil
//}
//
//func fetchJobPartStatus(jobId string , partNo string) (error){
//	url := "http://localhost:1337"
//	client := &http.Client{}
//	req, err := http.NewRequest("GET", url, nil)
//	if err != nil{
//		return err
//	}
//
//	q := req.URL.Query()
//	q.Add("type", "PartStatus")
//	q.Add("GUID", jobId)
//	q.Add("Part", partNo)
//	req.URL.RawQuery = q.Encode()
//
//	resp, err := client.Do(req)
//	if err != nil{
//		return err
//	}
//	if resp.StatusCode != http.StatusAccepted {
//		fmt.Println("request failed with status ", resp.Status)
//	}
//
//	defer resp.Body.Close()
//	body, err:= ioutil.ReadAll(resp.Body)
//	if err != nil{
//		return err
//	}
//	var status common.TransfersStatus
//	json.Unmarshal(body, &status)
//	for index := 0; index < len(status.Status); index++{
//		message := fmt.Sprintf("Source %s  Destination %s Status %d", status.Status[index].Src, status.Status[index].Dst, status.Status[index].Status)
//		fmt.Println(message)
//	}
//	return nil
//}
//
//func sendUploadRequestToSTE(guId string, partNumber uint32, sourceFileName string, targetfileName string) {
//	fmt.Println("Sending Upload Request TO STE")
//	url := "http://localhost:1337"
//	payload := common.CopyJobPartOrder{Version: 1,
//			ID: common.JobID(guId),
//			PartNum: common.PartNumber(partNumber),
//			Priority: HighJobPriority,
//			SourceType: common.Local,
//			DestinationType: common.Blob,
//			Transfers: []common.CopyTransfer{{sourceFileName, targetfileName, time.Now(), 12800340}},
//		OptionalAttributes: common.BlobTransferAttributes{BlockSizeinBytes: 4 * 1024 * 1024}}
//
//	payloadData, err := json.MarshalIndent(payload, "", "")
//	fmt.Println("Marshalled Data ", string(payloadData))
//	res, err := http.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payloadData))
//	if err != nil {
//		panic(err)
//	}
//	if err != nil {
//		panic(err)
//	}
//	defer res.Body.Close()
//	body, err := ioutil.ReadAll(res.Body)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("Response to request", res.Status, " ", body)
//	time.Sleep(5 * time.Second)
//	fetchJobPartStatus(guId, "0")
//}
//
//func sendStatusRequestToSTE(guid string, partNo string, transferIndex uint32, chunkIndex uint16){
//	guId,err := newUUID()
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("Sending Upload Request TO STE")
//	client := &http.Client{}
//	url := "http://localhost:1337"
//	payload := statusQuery{Guid:guId, PartNo:partNo, TransferIndex:transferIndex, ChunkIndex:chunkIndex}
//	payloadData, err := json.MarshalIndent(payload, "", "")
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("Marshalled Data ", string(payloadData))
//	req, err := http.NewRequest("PUT", url, bytes.NewReader(payloadData))
//	res, err := client.Do(req)
//	if err != nil {
//		panic(err)
//	}
//	if err != nil {
//		panic(err)
//	}
//	defer res.Body.Close()
//	body, err := ioutil.ReadAll(res.Body)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("Response to request", res.Status, " ", body)
//}
//
//func newUUID() (string, error) {
//	uuid := make([]byte, 16)
//	n, err := io.ReadFull(rand.Reader, uuid)
//	if n != len(uuid) || err != nil {
//		return "", err
//	}
//	// variant bits; see section 4.1.1
//	uuid[8] = uuid[8]&^0xc0 | 0x80
//	// version 4 (pseudo-random); see section 4.1.3
//	uuid[6] = uuid[6]&^0xf0 | 0x40
//	code,err := fmt.Sprintf("%x%x%x%x%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
//	fmt.Println(" Code", uuid)
//	return code, err
//}
//
//
//func main(){
//	fmt.Println("Welcome to the version of Project 0.0")
//
//	var commandGiven = ""
//	printCommand := flag.NewFlagSet("print" , flag.ExitOnError)
//	sourceFileName := printCommand.String("src", "", "File Name to upload")
//	targetFileName := printCommand.String("dst", "", "File Name to be copied to")
//
//	statusCommand := flag.NewFlagSet("status", flag.ExitOnError)
//	guid := statusCommand.String("guid", "", "")
//	partNo := statusCommand.String("part", "", "")
//
//	listCommand := flag.NewFlagSet("list", flag.ExitOnError)
//	jobId := listCommand.String("jobId", "", "")
//
//	if len(os.Args) < 1 {
//		fmt.Println("No Command Provided")
//		os.Exit(1)
//	}
//	if len(os.Args) == 1{
//		commandGiven = "normal"
//	}else{
//		commandGiven = os.Args[1]
//	}
//
//	switch commandGiven {
//	case "normal":
//		cmd := exec.Command("./AZCopy.exe", "StartSTE")
//		err := cmd.Start()
//		if err != nil{
//			panic(err)
//			os.Exit(1)
//		}
//	case "debug":
//		go func(){
//			//InitTransferEngine()
//		}()
//	case "print":
//		printCommand.Parse(os.Args[2:])
//		if printCommand.Parsed(){
//			if *targetFileName == "" {
//				printCommand.PrintDefaults()
//				os.Exit(1)
//			}
//			if *sourceFileName == "" {
//				printCommand.PrintDefaults()
//				os.Exit(1)
//			}
//		}
//		guId,err := newUUID()
//		if err != nil {
//			panic(err)
//		}
//		sendUploadRequestToSTE(guId, 0, *sourceFileName, *targetFileName)
//		//sendUploadRequestToSTE(guId, 1, *sourceFileName, *targetFileName)
//	case "status":
//		statusCommand.Parse(os.Args[2:])
//		if statusCommand.Parsed(){
//			if *guid == "" {
//				printCommand.PrintDefaults()
//				os.Exit(1)
//			}
//			if *partNo == "" {
//				fetchJobStatus(*guid)
//			}else {
//				fetchJobPartStatus(*guid, *partNo)
//			}
//		}
//	case "list":
//		listCommand.Parse(os.Args[2:])
//		if listCommand.Parsed(){
//			if *jobId == ""{
//				listActiveJobs()
//			}else{
//				fmt.Println("getting job details")
//				getJobDetails(*jobId)
//			}
//		}
//	case "StartSTE":
//		//InitTransferEngine()
//
//	default:
//		flag.PrintDefaults()
//		os.Exit(1)
//	}
//}
