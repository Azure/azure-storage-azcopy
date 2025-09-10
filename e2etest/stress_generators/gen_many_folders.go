package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
)

const (
	GenManyFoldersName = "many-folders"
)

func init() {
	RegisterGenerator(&ManyFoldersGenerator{})
}

type ManyFoldersGenerator struct {
	ContainerTarget  string
	preferredService string
}

func (m *ManyFoldersGenerator) PreferredService() common.Location {
	if m.preferredService != "" {
		var out common.Location
		_ = out.Parse(m.preferredService)
		return out
	}

	return common.ELocation.FileSMB()
}

func (m *ManyFoldersGenerator) Name() string {
	return GenManyFoldersName
}

func (m *ManyFoldersGenerator) Generate(manager e2etest.ServiceResourceManager) error {
	const (
		FolderSize  = 5
		MinDepth    = 5
		MaxDepth    = 10
		FolderCount = 1_000_000

		// mostly unused but preserved for the IDE hint
		TotalObjectCount = (FolderSize * FolderCount) + FolderCount
	)

	var depthMap = e2etest.NewRWMutexResource(make(map[int]int64))

	a := &DummyAsserter{}

	container := manager.GetContainer(m.ContainerTarget)

	if container.Exists() {
		return fmt.Errorf("please delete the container %s before re-running the generator", m.ContainerTarget)
	}

	container.Create(a, e2etest.ContainerProperties{})
	if a.CaughtError != nil {
		return fmt.Errorf("failed to create parent container %w", a.CaughtError)
	}

	gjm := NewGenerationJobManager(TotalObjectCount, CommonGenerateAnnouncementIncrement)
	gjm.CustomAnnounce = func() string {
		var out string

		depthMap.DoRead(func(res map[int]int64) {
			out = fmt.Sprint(res)
		})

		return "folder depths: " + out
	}

	// We limit the number of things we're trying to queue up at once so we don't casually use 64gb of memory for funsies
	allocationCap := semaphore.NewWeighted(100_000)

	genDirectoryName := func() string {
		var out string
		out = uuid.NewString()
		out = out[:strings.IndexRune(out, '-')]

		return out
	}

	cPath := make([]string, 0)

	trieMu := &sync.Mutex{}
	trie := e2etest.NewTrie[bool]('/')

	// should have a lock before calling genDirectoryNameSafe
	genDirectoryNameSafe := func() string {
		root := strings.Join(cPath, "/")
		var out string

		for {
			out = genDirectoryName()

			if trie.Get(root+"/"+out) == nil {
				break
			}
		}

		return out
	}

	trie.Insert(strings.Join(cPath, "/"), e2etest.PtrOf(true))
	for range FolderCount {
		for len(cPath) < MinDepth {
			cPath = append(cPath, genDirectoryNameSafe())
			trie.Insert(strings.Join(cPath, "/"), e2etest.PtrOf(true))
		}

		// generate the path out here
		folderPath := strings.Join(cPath, "/")

		// schedule creation
		gjm.ScheduleItem(func() error {
			a := &DummyAsserter{}

			depthMap.DoWrite(func(res map[int]int64) {
				res[len(strings.Split(folderPath, "/"))]++
			})

			if folderPath != "" {
				folder := container.GetObject(a, folderPath, common.EEntityType.Folder())
				folder.Create(a, e2etest.NewZeroObjectContentContainer(0), e2etest.ObjectProperties{})
				if a.CaughtError != nil {
					return fmt.Errorf("failed to create parent folder: %w", a.CaughtError)
				}
			}

			return nil
		}, false)

		for range FolderSize {
			_ = allocationCap.Acquire(context.Background(), 1)

			trieMu.Lock()
			filePath := folderPath + "/" + genDirectoryName()

			for e2etest.DerefOrZero(trie.Get(filePath)) {
				filePath = folderPath + "/" + genDirectoryName()
			}

			trie.Insert(filePath, e2etest.PtrOf(true))
			trieMu.Unlock()

			gjm.ScheduleItem(func() error {
				a := &DummyAsserter{}
				defer allocationCap.Release(1)

				file := container.GetObject(a, filePath, common.EEntityType.File())
				file.Create(a, e2etest.NewRandomObjectContentContainer(50), e2etest.ObjectProperties{})
				if a.CaughtError != nil {
					return fmt.Errorf("failed to create child object: %w", a.CaughtError)
				}

				return nil
			}, true) // create files with prio
		}

		trieMu.Lock()
		// path traversal, ensure we always get a new directory, and ensure there's some complexity to our tree.
		// random chance to ascend up the tree; or if we are at root we *must* ascend.
		if rNum := rand.IntN(101); (len(cPath) < MaxDepth && rNum > 60) || len(cPath) <= MinDepth {
			cPath = append(cPath, genDirectoryNameSafe())
		} else if rNum > 25 { // if we're 25-60, stay level, but switch to a new dir.
			cPath = cPath[:len(cPath)-1]
		} else {
			if len(cPath) == 1 {
				// descend to root if we're at 1
				cPath = []string{}
			} else {
				// if we are >=2, descend twice, then generate a new dir, putting us at n-1
				cPath = cPath[:len(cPath)-2]
				cPath = append(cPath, genDirectoryNameSafe())
			}
		}

		trie.Insert(strings.Join(cPath, "/"), e2etest.PtrOf(true))
		trieMu.Unlock()
	}

	gjm.Wait()

	if fc := atomic.LoadInt64(gjm.failureCount); fc > 0 {
		return fmt.Errorf("failed generating %d entries", fc)
	}

	return nil
}

func (m *ManyFoldersGenerator) RegisterFlags(pFlags *pflag.FlagSet) {
	pFlags.StringVar(&m.ContainerTarget, FlagContainerName, e2etest.SyntheticContainerManyFoldersSource, "Set a custom container name")
	pFlags.StringVar(&m.preferredService, FlagService, "", "Generate against a service other than the default (file)")
}
