package common

import (
	"strings"
)

type TrieNode struct {
	dirPathSeg             map[string]*TrieNode
	status                 uint32
	unregisteredButCreated bool
	isEnd                  bool
}

type Trie struct {
	Root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{
		Root: &TrieNode{dirPathSeg: make(map[string]*TrieNode)},
	}
}

// @brief InsertStatus inserts the status of the directory path in the trie
func (t *Trie) InsertStatus(dirPath string, dirCreationStatus uint32) {
	node := t.Root
	segments := strings.Split(dirPath, "/")
	for _, segment := range segments {
		child, exists := node.dirPathSeg[segment]
		if !exists {
			child = &TrieNode{dirPathSeg: make(map[string]*TrieNode)}
			node.dirPathSeg[segment] = child
		}
		node = child
	}
	node.status = dirCreationStatus
	node.isEnd = true
}

// @brief InsertUnregisteredStatus inserts the unregistered status of the directory path in the trie
func (t *Trie) SetUnregisteredStatus(dirPath string, unregisteredButCreated bool) {
	node := t.Root
	segments := strings.Split(dirPath, "/")
	for _, segment := range segments {
		child, exists := node.dirPathSeg[segment]
		if !exists {
			child = &TrieNode{dirPathSeg: make(map[string]*TrieNode)}
			node.dirPathSeg[segment] = child
		}
		node = child
	}

	node.unregisteredButCreated = unregisteredButCreated
	node.isEnd = true
}

// @brief GetStatus returns the status of the directory path in the trie
func (t *Trie) GetStatus(dirPath string) (uint32, bool) {
	node := t.Root
	segments := strings.Split(dirPath, "/")
	for _, segment := range segments {
		child, exists := node.dirPathSeg[segment]
		if !exists {
			return 0, false
		}
		node = child
	}
	if node.isEnd {
		return node.status, true
	}
	return 0, false
}

// @brief CheckIfUnregisteredButCreated checks if the directory path is unregistered but created in the trie
func (t *Trie) CheckIfUnregisteredButCreated(dirPath string) (bool, bool) {
	node := t.Root
	segments := strings.Split(dirPath, "/")
	for _, segment := range segments {
		child, exists := node.dirPathSeg[segment]
		if !exists {
			return false, false
		}
		node = child
	}
	if node.isEnd {
		return node.unregisteredButCreated, true
	}
	return false, false
}

// @brief GetDirDetails returns all the details of the directory path in the trie
func (t *Trie) GetDirDetails(dirPath string) (uint32, bool, bool) {
	node := t.Root
	segments := strings.Split(dirPath, "/")
	for _, segment := range segments {
		child, exists := node.dirPathSeg[segment]
		if !exists {
			return 0, false, false
		}
		node = child
	}
	if node.isEnd {
		return node.status, node.unregisteredButCreated, true
	}
	return 0, false, false
}
