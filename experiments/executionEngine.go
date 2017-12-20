package main

import (
	"fmt"
	"time"
)

type chunkJob struct {
	transferId int32
	blockId int32
}

type transferJob struct {
	id int32
	numOfBlocks int32
}

type suicideJob byte

// general purpose worker that reads in transfer jobs, schedules chunk jobs, and executes chunk jobs
func engineWorker(id int, highChunk chan chunkJob, highTransfer chan transferJob, suicideLine chan suicideJob) {
	for {
		// priority 0: whether to commit suicide
		select {
		case <-suicideLine:
			fmt.Println("Worker", id, "is committing SUICIDE.")
			return
		default:
			// priority 1: high priority chunk queue, do actual upload/download
			select  {
			case chunkJob := <- highChunk:
				fmt.Println("Worker", id, "is processing CHUNK job with transferId", chunkJob.transferId, "and blockID", chunkJob.blockId)
				time.Sleep(2 * time.Second)
			default:
				// priority 2: high priority transfer queue, schedule chunk jobs
				select {
				case transferJob := <- highTransfer:
					fmt.Println("Worker", id, "is processing TRANSFER job with id", transferJob.id)
					for i:=int32(1); i<=transferJob.numOfBlocks; i++ {
						highChunk <- chunkJob{ transferId:transferJob.id, blockId:i}
					}
				default:
					// lower priorities should go here in the future
					fmt.Println("Worker", id, "is IDLE, sleeping for 0.5 sec zzzzzz")
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}
}


func main() {
	fmt.Println("ENGINE STARTING!")

	highChunk := make(chan chunkJob, 100)
	highTransfer := make(chan transferJob, 100)
	suicideLine := make(chan suicideJob, 100)

	go engineWorker(1, highChunk, highTransfer, suicideLine)
	go engineWorker(2, highChunk, highTransfer, suicideLine)
	highTransfer <- transferJob{ numOfBlocks: 10, id: 1}
	highTransfer <- transferJob{ numOfBlocks: 10, id: 2}
	highTransfer <- transferJob{ numOfBlocks: 10, id: 3}

	// wait a bit and kill one worker
	time.Sleep(10 * time.Second)
	suicideLine <- 0

	// wait a bit and add one worker
	time.Sleep(10 * time.Second)
	fmt.Println("NEW WORKER IN TOWN!")
	go engineWorker(3, highChunk, highTransfer, suicideLine)

	// let the execution engine run
	time.Sleep(50 * time.Second)
}
