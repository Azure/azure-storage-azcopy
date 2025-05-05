package main

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

/*
This is really slapdash
*/

var (
	ThreadCount = func() int64 {
		threadCount := int64(runtime.NumCPU()) * 4

		if threadCount > 64 {
			return 64 // mirror AzCopy's thread count
		}

		return threadCount
	}()

	PriorityJobQueue = make(chan func(), ThreadCount)
	JobQueue         = make(chan func(), ThreadCount)
)

func init() {
	for range ThreadCount {
		go func() {

			var (
				j  func()
				ok bool
			)

			for {
				select {
				case j, ok = <-PriorityJobQueue:
				case j, ok = <-JobQueue:
				}

				if j != nil {
					j()
					j = nil
				}

				if !ok {
					return
				}
			}
		}()
	}
}

func NewGenerationJobManager(itemCount int, statusIncrement int64) *GenerationJobManager {
	out := &GenerationJobManager{
		wg:              &sync.WaitGroup{},
		totalCount:      e2etest.PtrOf[int64](int64(itemCount)),
		doneCount:       e2etest.PtrOf[int64](0),
		failureCount:    e2etest.PtrOf[int64](0),
		statusIncrement: statusIncrement,
	}

	out.wg.Add(itemCount)

	return out
}

type GenerationJobManager struct {
	wg              *sync.WaitGroup
	totalCount      *int64
	doneCount       *int64
	failureCount    *int64
	statusIncrement int64

	CustomAnnounce func() string
}

func (gjm *GenerationJobManager) ScheduleAdditionalItems(n int) {
	gjm.wg.Add(n)
	atomic.AddInt64(gjm.totalCount, int64(n))
}

func (gjm *GenerationJobManager) ScheduleItem(f func() error, prio bool) {
	go func() {
		f := func() {
			defer gjm.wg.Done()

			err := f()
			if err != nil {
				atomic.AddInt64(gjm.failureCount, 1)
				fmt.Printf("failed to create an object: %s\n", err)
			} else {
				done := atomic.AddInt64(gjm.doneCount, 1)
				if done%gjm.statusIncrement == 0 {
					cAnnounce := func() string {
						if gjm.CustomAnnounce == nil {
							return ""
						}

						return " " + gjm.CustomAnnounce()
					}

					fmt.Printf("%v: generated %d items of %d!%v\n", time.Now().Format(time.Stamp), done, atomic.LoadInt64(gjm.totalCount), cAnnounce())
				}
			}
		}

		if prio {
			PriorityJobQueue <- f
		} else {
			JobQueue <- f
		}
	}()
}

func (gjm *GenerationJobManager) Wait() {
	gjm.wg.Wait()
}
