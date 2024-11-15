package common

import (
	"strings"
)

type TrieNode struct {
	parent        *TrieNode
	children      map[string]*TrieNode
	TransferIndex uint32
	Status        DirCreationStatus
}

type DirCreationStatus uint8

const (
	DirCreationStatusRegistered DirCreationStatus = 1 << iota // It has a valid tx index.
	DirCreationStatusExists                                   // It exists, either because of us or someone else.
	DirCreationStatusCreated                                  // It exists, because of us.

	DirCreationStatusNil DirCreationStatus = 0 // Nil-- We don't know if it's there or not. This is a good zero state. Unregistered.
)

func DirCreationBitsForStatus(status TransferStatus) (out DirCreationStatus) {
	out |= DirCreationStatusRegistered // We have a TransferStatus, certainly from a real tx.
	s := ETransferStatus

	switch status {
	case s.Success(), s.FolderCreated():
		out |= DirCreationStatusCreated // That's us
	case s.SkippedEntityAlreadyExists():
		out |= DirCreationStatusExists // That's them
	}

	return
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
				child = &TrieNode{children: make(map[string]*TrieNode), parent: node}
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
