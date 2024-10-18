package ste

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type TrieNode struct {
	children map[rune]*TrieNode
	value    *uint32
	isEnd    bool
}

type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{
		root: &TrieNode{children: make(map[rune]*TrieNode)},
	}
}

func (t *Trie) Insert(key string, value uint32) {
	node := t.root
	for _, char := range key {
		if _, exists := node.children[char]; !exists {
			node.children[char] = &TrieNode{children: make(map[rune]*TrieNode)}
		}
		node = node.children[char]
	}
	node.value = &value
	node.isEnd = true
}

func (t *Trie) Get(key string) (*uint32, bool) {
	node := t.root
	for _, char := range key {
		if _, exists := node.children[char]; !exists {
			return nil, false
		}
		node = node.children[char]
	}
	if node.isEnd {
		return node.value, true
	}
	return nil, false
}

func (t *Trie) Delete(key string) bool {
	return t.deleteHelper(t.root, key, 0)
}

func (t *Trie) deleteHelper(node *TrieNode, key string, depth int) bool {
	if node == nil {
		return false
	}

	// If we have reached the end of the key
	if depth == len(key) {
		if !node.isEnd {
			return false // Key does not exist
		}
		node.isEnd = false
		node.value = nil

		// If the node has no children, it can be deleted
		return len(node.children) == 0
	}

	char := rune(key[depth])
	if t.deleteHelper(node.children[char], key, depth+1) {
		delete(node.children, char)
		return !node.isEnd && len(node.children) == 0
	}

	return false
}

type FolderCreationTracker common.FolderCreationTracker

type JPPTCompatibleFolderCreationTracker interface {
	FolderCreationTracker
	RegisterPropertiesTransfer(folder string, transferIndex uint32)
}

func NewFolderCreationTracker(fpo common.FolderPropertyOption, plan *JobPartPlanHeader) FolderCreationTracker {
	switch fpo {
	case common.EFolderPropertiesOption.AllFolders(),
		common.EFolderPropertiesOption.AllFoldersExceptRoot():
		return &jpptFolderTracker{ // This prevents a dependency cycle. Reviewers: Are we OK with this? Can you think of a better way to do it?
			plan:                   plan,
			mu:                     &sync.Mutex{},
			contents:               NewTrie(),
			unregisteredButCreated: make(map[string]struct{}),
		}
	case common.EFolderPropertiesOption.NoFolders():
		// can't use simpleFolderTracker here, because when no folders are processed,
		// then StopTracking will never be called, so we'll just use more and more memory for the map
		return &nullFolderTracker{}
	default:
		panic("unknown folderPropertiesOption")
	}
}

type nullFolderTracker struct{}

func (f *nullFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	// no-op (the null tracker doesn't track anything)
	return doCreation()
}

func (f *nullFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	// There's no way this should ever be called, because we only create the nullTracker if we are
	// NOT transferring folder info.
	panic("wrong type of folder tracker has been instantiated. This type does not do any tracking")
}

func (f *nullFolderTracker) StopTracking(folder string) {
	// noop (because we don't track anything)
}

type jpptFolderTracker struct {
	plan                   IJobPartPlanHeader
	mu                     *sync.Mutex
	contents               *Trie
	unregisteredButCreated map[string]struct{}
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	print("Registering folder: " + folder + "\n")

	if folder == common.Dev_Null {
		return // Never persist to dev-null
	}

	f.contents.Insert(folder, transferIndex)

	// We created it before it was enumerated-- Let's register that now.
	if _, ok := f.unregisteredButCreated[folder]; ok {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)

		delete(f.unregisteredButCreated, folder)
	}
}

func (f *jpptFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return nil // Never persist to dev-null
	}

	if idx, ok := f.contents.Get(folder); ok {
		status := f.plan.Transfer(*idx).TransferStatus()
		if status == (common.ETransferStatus.FolderCreated()) || status == (common.ETransferStatus.Success()) {
			return nil
		}
	}

	if _, ok := f.unregisteredButCreated[folder]; ok {
		return nil
	}

	err := doCreation()
	if err != nil {
		return err
	}

	if idx, ok := f.contents.Get(folder); ok {
		// overwrite it's transfer status
		f.plan.Transfer(*idx).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	} else {
		// A folder hasn't been hit in traversal yet.
		// Recording it in memory is OK, because we *cannot* resume a job that hasn't finished traversal.
		f.unregisteredButCreated[folder] = struct{}{}
	}

	return nil
}

func (f *jpptFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	if folder == common.Dev_Null {
		return false // Never persist to dev-null
	}

	switch overwrite {
	case common.EOverwriteOption.True():
		return true
	case common.EOverwriteOption.Prompt(),
		common.EOverwriteOption.IfSourceNewer(),
		common.EOverwriteOption.False():

		f.mu.Lock()
		defer f.mu.Unlock()

		var created bool
		if idx, ok := f.contents.Get(folder); ok {
			created = f.plan.Transfer(*idx).TransferStatus() == common.ETransferStatus.FolderCreated()
		} else {
			// This should not happen, ever.
			// Folder property jobs register with the tracker before they start getting processed.
			panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("folder " + folder + " was not registered when properties persistence occurred"))
		}

		// prompt only if we didn't create this folder
		if overwrite == common.EOverwriteOption.Prompt() && !created {
			cleanedFolderPath := folder

			// if it's a local Windows path, skip since it doesn't have SAS and won't parse correctly as an URL
			if !strings.HasPrefix(folder, common.EXTENDED_PATH_PREFIX) {
				// get rid of SAS before prompting
				parsedURL, _ := url.Parse(folder)

				// really make sure that it's not a local path
				if parsedURL.Scheme != "" && parsedURL.Host != "" {
					parsedURL.RawQuery = ""
					cleanedFolderPath = parsedURL.String()
				}
			}
			return prompter.ShouldOverwrite(cleanedFolderPath, common.EEntityType.Folder())
		}

		return created
	default:
		panic("unknown overwrite option")
	}
}

func (f *jpptFolderTracker) StopTracking(folder string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return // Not possible to track this
	}

	//add a log mentioning we are in stotracking and folder is being deleted
	fmt.Println("In StopTracking, deleting folder: " + folder)

	// no-op, because tracking is now handled by jppt, anyway.
	if f.contents != nil {
		if _, ok := f.contents.Get(folder); ok {
			f.contents.Delete(folder)
		} else {
			currentContents := ""

			for k, v := range f.contents.root.children {
				currentContents += fmt.Sprintf("K: %c V: %v\n", k, v.value)
			}

			// double should never be hit, but *just in case*.
			panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded\nCurrent Contents:\n" + currentContents))
		}
	}
}
