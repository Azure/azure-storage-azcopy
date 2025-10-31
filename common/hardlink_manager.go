package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

type FileStatus string

const (
	StatusPending     FileStatus = "Pending"
	StatusTransferred FileStatus = "Transferred"
	StatusLinked      FileStatus = "Linked"
	StatusFailed      FileStatus = "Failed"
)

type TrieNode struct {
	Name     string
	Children map[string]*TrieNode
	InodeKey string
	Status   FileStatus
	IsFile   bool
}

type HardlinkTrie struct {
	Root     *TrieNode
	InodeMap map[string][]string // inodeKey -> file paths
	Status   map[string]FileStatus
	Mu       sync.Mutex
	stopChan chan struct{}
}

func NewHardlinkTrie() *HardlinkTrie {
	return &HardlinkTrie{
		Root:     &TrieNode{Name: "/", Children: make(map[string]*TrieNode)},
		InodeMap: make(map[string][]string),
		Status:   make(map[string]FileStatus),
		stopChan: make(chan struct{}),
	}
}

func (ht *HardlinkTrie) StopChan() {
	close(ht.stopChan)
}

// Insert a file path and its inodeKey into the Trie.
// Returns true if this is the first occurrence of the inode.
func (ht *HardlinkTrie) RegisterHardlink(filePath string, fileInfo os.FileInfo) (isFirst bool) {
	ht.Mu.Lock()
	defer ht.Mu.Unlock()

	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		// Not a POSIX-compatible file or missing stat info
		return false
	}

	inodeKey := fmt.Sprintf("%d-%d", stat.Dev, stat.Ino)

	parts := strings.Split(filePath, "/")
	node := ht.Root

	for i, part := range parts {
		if part == "" {
			continue // skip empty entries (like the first one in absolute paths)
		}
		if node.Children == nil {
			node.Children = make(map[string]*TrieNode)
		}
		child, exists := node.Children[part]
		if !exists {
			child = &TrieNode{Name: part, Children: make(map[string]*TrieNode)}
			node.Children[part] = child
		}
		node = child

		if i == len(parts)-1 {
			node.IsFile = true
			node.InodeKey = inodeKey
			node.Status = StatusPending
		}
	}

	_, exists := ht.InodeMap[inodeKey]
	if !exists {
		ht.InodeMap[inodeKey] = []string{filePath}
		ht.Status[inodeKey] = StatusPending
		isFirst = true
	} else {
		ht.InodeMap[inodeKey] = append(ht.InodeMap[inodeKey], filePath)
		isFirst = false
	}

	return
}

// Mark file as transferred
func (ht *HardlinkTrie) MarkStatus(filePath string, status FileStatus) {
	ht.Mu.Lock()
	defer ht.Mu.Unlock()

	parts := strings.Split(strings.TrimPrefix(filePath, "/"), "/")

	node := ht.Root
	for _, part := range parts {
		node = node.Children[part]
		if node == nil {
			return
		}
	}
	if node.IsFile {
		node.Status = status
		ht.Status[node.InodeKey] = status
	}
}

// Save inode map to JSON periodically
func (ht *HardlinkTrie) saveJSON(filePath string) error {
	ht.Mu.Lock()
	defer ht.Mu.Unlock()

	data := make(map[string]map[string]interface{})
	for inode, paths := range ht.InodeMap {
		statusMap := make(map[string]FileStatus)
		for _, p := range paths {
			// Look up this file’s node status from the trie
			parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
			node := ht.Root
			for _, part := range parts {
				child, ok := node.Children[part]
				if !ok {
					continue
				}
				node = child
			}
			if node != nil && node.IsFile {
				statusMap[p] = node.Status
			} else {
				statusMap[p] = StatusPending // fallback if not found
			}
		}
		data[inode] = map[string]interface{}{
			"Paths":    paths,
			"Statuses": statusMap,
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// saveToJSON periodically writes all hardlink groups to file
func SaveToJSONPeriodically(filename string, interval time.Duration) {
	filepath := path.Join(AzcopyJobPlanFolder, filename)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			HardlinkNode.saveJSON(filepath)
		case <-HardlinkNode.stopChan:
			HardlinkNode.saveJSON(filepath)
			return
		}
	}
}

var HardlinkNode = NewHardlinkTrie()

// PrintAll prints the entire HardlinkTrie contents in a structured format.
func (t *HardlinkTrie) PrintAll() {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	fmt.Println("========== HARDLINK TRIE STRUCTURE ==========")
	printTrie(t.Root, 0)

	fmt.Println("\n========== INODE MAP ==========")
	for inode, paths := range t.InodeMap {
		fmt.Printf("Inode: %s\n", inode)
		for _, p := range paths {
			fmt.Printf("  - %s\n", p)
		}
	}

	fmt.Println("\n========== FILE STATUSES ==========")
	for path, status := range t.Status {
		fmt.Printf("%-50s : %v\n", path, status)
	}
}

// Helper: Recursively print the Trie
func printTrie(node *TrieNode, depth int) {
	if node == nil {
		return
	}

	indent := strings.Repeat("  ", depth)
	nodeType := "Dir"
	if node.IsFile {
		nodeType = "File"
	}
	fmt.Printf("%s[%s] %s (Inode: %s, Status: %v)\n", indent, nodeType, node.Name, node.InodeKey, node.Status)

	for _, child := range node.Children {
		printTrie(child, depth+1)
	}
}
