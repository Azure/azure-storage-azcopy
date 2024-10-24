package common

import (
	"strings"
)

type TrieNode struct {
	children               map[string]*TrieNode
	TransferIndex          uint32
	UnregisteredButCreated bool
}

type Trie struct {
	Root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{
		Root: &TrieNode{children: make(map[string]*TrieNode)},
	}
}

// InsertDirNode inserts the dirPath into the Trie and returns the corresponding node and if it had to be created
func (t *Trie) InsertDirNode(dirPath string) (*TrieNode, bool) {
	node, _, created := t.getDirNodeHelper(dirPath, true)
	return node, created
}

// GetDirNode returns the directory node if it exists
func (t *Trie) GetDirNode(dirPath string) (*TrieNode, bool) {
	node, exists, _ := t.getDirNodeHelper(dirPath, false)
	return node, exists
}

// getDirNodeHelper returns the node, if the node exists and if the node had to be created
func (t *Trie) getDirNodeHelper(dirPath string, createIfNotExists bool) (*TrieNode, bool, bool) {
	node := t.Root
	segments := strings.Split(dirPath, "/")
	created := false
	for _, segment := range segments {
		child, exists := node.children[segment]
		if !exists {
			if createIfNotExists {
				child = &TrieNode{children: make(map[string]*TrieNode)}
				node.children[segment] = child
				created = true
			} else {
				return nil, false, false
			}
		}
		node = child
	}
	return node, true, created
}
