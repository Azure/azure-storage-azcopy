package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
	"sync/atomic"
)

const (
	GenManyObjectsCount       = 10_000_000
	GenManyObjectsLogDivision = 10_000
)

func init() {
	RegisterGenerator(&ManyObjectsGenerator{})
}

type ManyObjectsGenerator struct {
	ContainerTarget  string
	requestedService string
}

func (m *ManyObjectsGenerator) PreferredService() common.Location {
	if m.requestedService != "" {
		var out common.Location
		_ = out.Parse(m.requestedService) // if this fails it'll be pretty obvious
		return out
	}

	return common.ELocation.Blob()
}

func (m *ManyObjectsGenerator) RegisterFlags(pFlags *pflag.FlagSet) {
	pFlags.StringVar(&m.ContainerTarget, FlagContainerName, e2etest.SyntheticContainerManyFilesSource, "Set a custom container name")
	pflag.StringVar(&m.requestedService, FlagService, "", "Use a custom service instead of the default (blob)")
}

func (m *ManyObjectsGenerator) Name() string {
	return e2etest.SyntheticContainerManyFilesSource
}

func (m *ManyObjectsGenerator) Generate(service e2etest.ServiceResourceManager) error {
	a := &DummyAsserter{}

	container := service.GetContainer(m.ContainerTarget)

	if container.Exists() {
		return fmt.Errorf("please delete the container %s before re-running the generator", m.ContainerTarget)
	}

	container.Create(a, e2etest.ContainerProperties{})
	if a.CaughtError != nil {
		return fmt.Errorf("failed to create container: %w", a.CaughtError)
	}

	gjm := NewGenerationJobManager(GenManyObjectsCount, GenManyObjectsLogDivision)
	// We limit the number of things we're trying to queue up at once so we don't casually use 64gb of memory for funsies
	allocationCap := semaphore.NewWeighted(100_000)

	for range GenManyObjectsCount {
		_ = allocationCap.Acquire(context.Background(), 1)
		gjm.ScheduleItem(func() error {
			// shadow the asserter
			a := &DummyAsserter{}

			defer allocationCap.Release(1)

			// make the object
			obj := container.GetObject(a, uuid.NewString(), common.EEntityType.File())
			obj.Create(a, e2etest.NewRandomObjectContentContainer(1024), e2etest.ObjectProperties{})
			if a.CaughtError != nil {
				return fmt.Errorf("failed to create object: %s\n", a.CaughtError)
			}

			return nil
		}, true)
	}

	gjm.Wait()

	if fc := atomic.LoadInt64(gjm.failureCount); fc > 0 {
		return fmt.Errorf("failed generating %d entries", fc)
	}

	return nil
}
