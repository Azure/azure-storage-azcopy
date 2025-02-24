package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	GenManyObjectsName = "many-objects"
)

func init() {
	RegisterGenerator(&ManyObjectsGenerator{})
}

type ManyObjectsGenerator struct {
	ContainerTarget string
}

func (m ManyObjectsGenerator) RegisterFlags(pFlags *flag.FlagSet) {
	pFlags.StringVar(&m.ContainerTarget, FlagContainerName, e2etest.SyntheticContainerManyFilesSource, "Set a custom container name")
}

func (m ManyObjectsGenerator) Name() string {
	return GenManyObjectsName
}

func (m ManyObjectsGenerator) Generate(service e2etest.ServiceResourceManager) error {
	a := &DummyAsserter{}

	container := service.GetContainer(m.ContainerTarget)

	container.Create(a, e2etest.ContainerProperties{})
	if a.CaughtError != nil {
		return fmt.Errorf("failed to create container: %w", a.CaughtError)
	}

	threadPool := semaphore.NewWeighted(int64(runtime.NumCPU()))
	waitGroup := &sync.WaitGroup{}
	doneCount := e2etest.PtrOf[int64](0)
	failures := e2etest.PtrOf[int64](0)

	// object count is not adjustable, this is generating a scenario for data purposes.
	objCount := 10_000_000
	waitGroup.Add(objCount)
	for objNum := 0; objNum < 10_000_000; objNum++ {

		_ = threadPool.Acquire(context.Background(), 1) // can't fail because there's no cancel
		go func() {
			defer waitGroup.Done()
			defer threadPool.Release(1)

			// shadow the asserter
			a := &DummyAsserter{}

			// make the object
			obj := container.GetObject(a, uuid.NewString(), common.EEntityType.File())
			obj.Create(a, e2etest.NewRandomObjectContentContainer(1024), e2etest.ObjectProperties{})
			if a.CaughtError != nil {
				atomic.AddInt64(failures, 1)
				fmt.Printf("failed to create object: %s", a.CaughtError)
			}

			counter := atomic.AddInt64(doneCount, 1)
			if counter%10_000 == 0 {
				// give the user some sorta useful output
				fmt.Println("generated", objNum)
			}
		}()
	}

	return nil
}
