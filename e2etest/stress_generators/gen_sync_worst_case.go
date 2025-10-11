package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
)

const (
	GenSyncWorstCaseName = "sync-worst-case"
)

func init() {
	RegisterGenerator(&SyncWorstCaseGenerator{})
}

type SyncWorstCaseGenerator struct {
	SourceContainerTarget string
	DestContainerTarget   string
	preferredService      string
}

func (s *SyncWorstCaseGenerator) PreferredService() common.Location {
	if s.preferredService != "" {
		var out common.Location
		_ = out.Parse(s.preferredService)
		return out
	}

	return common.ELocation.Blob()
}

func (s *SyncWorstCaseGenerator) Name() string {
	return GenSyncWorstCaseName
}

func (s *SyncWorstCaseGenerator) Generate(manager e2etest.ServiceResourceManager) error {
	a := &DummyAsserter{}

	sourceCt := manager.GetContainer(s.SourceContainerTarget)
	destCt := manager.GetContainer(s.DestContainerTarget)

	if sourceCt.Exists() || destCt.Exists() {
		return fmt.Errorf("please delete both containers %s and %s before re-running the generator", s.SourceContainerTarget, s.DestContainerTarget)
	}

	sourceCt.Create(a, e2etest.ContainerProperties{})
	if a.CaughtError != nil {
		return fmt.Errorf("failed to create source container: %w", a.CaughtError)
	}

	destCt.Create(a, e2etest.ContainerProperties{})
	if a.CaughtError != nil {
		return fmt.Errorf("failed to create source container: %w", a.CaughtError)
	}

	/*
		The worst-case scenario for almost any implementation of sync's algorithm is "simple". A big, flat folder, deep in a tree, with no overlaps on either side.
		However, for a real test of optimization, we actually want some overlap. Despite this being named "worst case", it's more intended to capture the "worst cases" of a real scenario.

		We might see cases of deeply-nested folders filled with large numbers of unique items. But we might also see overlaps. We might see smaller directories.
	*/
	const (
		MinDepth              = 2
		MaxDepth              = 7
		TotalFileCount        = 10_000_000
		IncrementAnnouncement = TotalFileCount / 1_000

		FilesPerDir = TotalFileCount / (MaxDepth - MinDepth)
		// Our bottom directory will overlap 100%, but our top will overlap 1/nth, testing a good variety of cases.
		OverlapIncrement = FilesPerDir / (MaxDepth - MinDepth)
	)

	gjm := NewGenerationJobManager(TotalFileCount+(MaxDepth), IncrementAnnouncement)

	cDir := make([]string, 0)
	currentOverlap := OverlapIncrement * (MaxDepth - MinDepth)

	fileContent := e2etest.NewRandomObjectContentContainer(100)
	// We limit the number of things we're trying to queue up at once so we don't casually use 64gb of memory for funsies
	allocationCap := semaphore.NewWeighted(100_000)

	createFiles := func(sourcePath, destPath string) {
		_ = allocationCap.Acquire(context.Background(), 1)

		gjm.ScheduleItem(func() error {
			a := &DummyAsserter{}

			defer allocationCap.Release(1)

			sourceFile := sourceCt.GetObject(a, sourcePath, common.EEntityType.File())
			if a.CaughtError != nil {
				return fmt.Errorf("failed to get source file: %w", a.CaughtError)
			}
			destFile := destCt.GetObject(a, destPath, common.EEntityType.File())
			if a.CaughtError != nil {
				return fmt.Errorf("failed to get dest file: %w", a.CaughtError)
			}

			sourceFile.Create(a, fileContent, e2etest.ObjectProperties{})
			if a.CaughtError != nil {
				return fmt.Errorf("failed to create source file: %w", a.CaughtError)
			}
			destFile.Create(a, fileContent, e2etest.ObjectProperties{})
			if a.CaughtError != nil {
				return fmt.Errorf("failed to create source file: %w", a.CaughtError)
			}

			return nil
		}, false)
	}

	for k := range MaxDepth {
		dirPath := strings.Join(cDir, "/")

		if len(cDir) > 0 {
			gjm.ScheduleItem(func() error {
				a := &DummyAsserter{}

				srcFolder := sourceCt.GetObject(a, dirPath, common.EEntityType.Folder())
				if a.CaughtError != nil {
					return fmt.Errorf("failed to get source folder: %w", a.CaughtError)
				}
				srcFolder.Create(a, e2etest.NewZeroObjectContentContainer(0), e2etest.ObjectProperties{})
				if a.CaughtError != nil {
					return fmt.Errorf("failed to create source folder: %w", a.CaughtError)
				}

				dstFolder := destCt.GetObject(a, dirPath, common.EEntityType.Folder())
				if a.CaughtError != nil {
					return fmt.Errorf("failed to get dest folder: %w", a.CaughtError)
				}
				dstFolder.Create(a, e2etest.NewZeroObjectContentContainer(0), e2etest.ObjectProperties{})
				if a.CaughtError != nil {
					return fmt.Errorf("failed to create dest folder: %w", a.CaughtError)
				}

				return nil
			}, true)
		}

		if k < MinDepth {
			goto stepUp
		}

		// First, create the overlap files
		for range currentOverlap {
			f := uuid.NewString()
			if len(dirPath) > 0 {
				f = dirPath + "/" + f
			}
			createFiles(f, f)
		}

		// Then, if any remain, create the non-overlapping files
		for range FilesPerDir - currentOverlap {
			pfx := ""
			if len(dirPath) > 0 {
				pfx = dirPath + "/"
			}

			createFiles(pfx+uuid.NewString(), pfx+uuid.NewString())
		}

		// drop the overlap moving up
		currentOverlap -= OverlapIncrement
	stepUp:
		cDir = append(cDir, uuid.NewString())
	}

	gjm.Wait()

	if fc := atomic.LoadInt64(gjm.failureCount); fc > 0 {
		return fmt.Errorf("failed generating %d entries", fc)
	}

	return nil
}

func (s *SyncWorstCaseGenerator) RegisterFlags(flags *pflag.FlagSet) {
	flags.StringVar(&s.SourceContainerTarget, FlagSourceContainerName, e2etest.SyntheticContainerSyncWorstCaseSource, "Specify a target container for the source")
	flags.StringVar(&s.DestContainerTarget, FlagDestContainerName, e2etest.SyntheticContainerSyncWorstCaseDest, "Specify a target container for the destination")
	flags.StringVar(&s.preferredService, FlagService, "", "Generate against a service other than the default (blob)")
}
