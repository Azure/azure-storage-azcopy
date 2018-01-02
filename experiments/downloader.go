package main
//
//import "fmt"
//import (
//	"github.com/edsrzf/mmap-go"
//	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
//	"os"
//	"path/filepath"
//	"net/url"
//	"context"
//	"io"
//	"time"
//)
//
//const(
//	chunkSize int64 = 4 * 1024 * 1024
//)
//
//type readerWrapper struct {
//	source io.Reader
//}
//
//var DeadlineExceeded error = deadlineExceededError{}
//
//type deadlineExceededError struct{}
//
//func (deadlineExceededError) Error() string   { return "context deadline exceeded; consider increasing RetryOptions' TryTimeout field when creating a pipeline" }
//func (deadlineExceededError) Timeout() bool   { return true }
//func (deadlineExceededError) Temporary() bool { return true }
//
//func (r *readerWrapper) Read(p []byte) (n int, err error){
//	bytesRead, err := r.source.Read(p)
//
//	if err == context.DeadlineExceeded {
//		err = DeadlineExceeded
//	}
//
//	return bytesRead, err
//}
//
//func main() {
//	// time the entire download operation
//	t1 := time.Now()
//	bytesDownloaded := DownloadBlobToFile("https://azcopynextgendev2.blob.core.windows.net/testcontainer/Testy_PPT1.pptx?st=2017-12-07T00%3A27%3A00Z&se=2018-12-08T00%3A27%3A00Z&sp=rwdl&sv=2016-05-31&sr=c&sig=D9xT4VAKVAHQYosYzKDY%2FaMhBrTIvlcxLORsPst6%2BuM%3D",
//		filepath.Join("/Users/Zed/Documents/test-download", "Testy_PPT1.pptx"))
//	t2 := time.Now()
//
//	// calculate time and speed to report
//	downloadDuration := t2.Sub(t1)
//	averageSpeed := float64(bytesDownloaded)/downloadDuration.Seconds()/1024/1024
//	fmt.Println("\nDownload took ", downloadDuration)
//	fmt.Println("\nAverage speed was:", averageSpeed, "MB/s")
//}
//
//func DownloadBlobToFile(blobUrlString, destinationPath string) int64{
//	// -----PROLOGUE-----
//
//	// get blob size
//	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
//		Retry: azblob.RetryOptions{
//			Policy:        azblob.RetryPolicyExponential,
//			MaxTries:      3,
//			TryTimeout:    time.Second * 60,
//			RetryDelay:    time.Second * 1,
//			MaxRetryDelay: time.Second * 3,
//		},
//	})
//	u, _ := url.Parse(blobUrlString)
//	blobUrl := azblob.NewBlobURL(*u, p)
//	blobSize := getBlobSize(blobUrl)
//	fmt.Printf("Blob download triggered: size=%v", blobSize)
//
//	// prep local file before download starts
//	memoryMappedFile := openAndMemoryMapFile(destinationPath, blobSize)
//
//	// initialize worker pool with 10 workers
//	jobs := make(chan job, 100)
//	results := make(chan job, 100)
//	for w := 1; w <= 10; w++ {
//		go downloadWorker(w, jobs, results)
//	}
//
//	// -----MAIN STORY-----
//	numOfJobs := 0
//
//	// download blob in parallel
//	for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
//		count :=  chunkSize
//
//		// compute range
//		if startIndex + chunkSize > blobSize{
//			count = blobSize - startIndex
//		}
//
//		numOfJobs += 1
//		jobs <- job{blobUrl:&blobUrl, memoryMappedFile:memoryMappedFile[startIndex: startIndex + count], startIndex:startIndex, count:count}
//	}
//
//	// wait until all jobs finish
//	close(jobs)
//	for a := 1; a <= numOfJobs; a++ {
//		<-results // todo make byte
//	}
//
//	// -----EPILOGUE-----
//	err := memoryMappedFile.Unmap()
//	if err != nil {
//		panic(err)
//	}
//	return blobSize
//}
//
//type job struct {
//	blobUrl *azblob.BlobURL
//	memoryMappedFile []byte
//	startIndex int64
//	count      int64
//}
//
//func downloadWorker(id int, jobs <-chan job, results chan<- job) {
//	ctx := context.Background()
//
//	for j := range jobs {
//		fmt.Printf("\nworker %v is getting block: start=%v, count=%v", id, j.startIndex, j.count)
//
//		// get a range of the blob
//		get, err := j.blobUrl.GetBlob(ctx, azblob.BlobRange{Offset: j.startIndex, Count: j.count}, azblob.BlobAccessConditions{}, false)
//		if err != nil {
//			panic(fmt.Sprintf("Download failed while getting block with: start=%v, count=%v", j.startIndex, j.count))
//		}
//
//		// write the body into memory mapped file directly
//		wrappedNetworkStream := &readerWrapper{source: get.Body()}
//		bytesRead, err := io.ReadFull(wrappedNetworkStream, j.memoryMappedFile)
//		if int64(bytesRead) != j.count || err != nil {
//			panic(fmt.Sprintf("Failed to write block to file with: start=%v, count=%v, actualWritten=%v err=%v", j.startIndex, j.count, bytesRead, err))
//		}
//
//		fmt.Printf("\nworker %v finished job with starting index=%v", id, j.startIndex)
//		results <- j
//	}
//}
//
//// opens file with desired flags and return File
//func openFile(destinationPath string, flags int) *os.File {
//	f, err := os.OpenFile(destinationPath, flags, 0644)
//	if err != nil {
//		panic(err.Error())
//	}
//	return f
//}
//
//// create/open and memory map a file, given its path and length
//func openAndMemoryMapFile(destinationPath string, fileSize int64) mmap.MMap {
//	f := openFile(destinationPath, os.O_RDWR | os.O_CREATE | os.O_TRUNC)
//	if truncateError := f.Truncate(fileSize); truncateError != nil {
//		panic(truncateError)
//	}
//
//	memoryMappedFile, err := mmap.Map(f, mmap.RDWR, 0)
//	if err != nil {
//		panic(fmt.Sprintf("Error mapping: %s", err))
//	}
//	return memoryMappedFile
//}
//
//// make a HEAD request to get the blob size
//func getBlobSize(blobUrl azblob.BlobURL) int64{
//	blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
//	if err != nil {
//		panic("Cannot get blob size")
//	}
//	return blobProperties.ContentLength()
//}