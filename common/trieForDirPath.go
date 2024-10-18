package common

import (
	"strings"
)

type TrieNode struct {
	Children map[string]*TrieNode
	Value    uint32
	isEnd    bool
}

type Trie struct {
	Root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{
		Root: &TrieNode{Children: make(map[string]*TrieNode)},
	}
}

func (t *Trie) Insert(key string, value uint32) {
	node := t.Root
	segments := strings.Split(key, "/")
	for _, segment := range segments {
		child, exists := node.Children[segment]
		if !exists {
			child = &TrieNode{Children: make(map[string]*TrieNode)}
			node.Children[segment] = child
		}
		node = child
	}
	node.Value = value
	node.isEnd = true
}

func (t *Trie) Get(key string) (uint32, bool) {
	node := t.Root
	segments := strings.Split(key, "/")
	for _, segment := range segments {
		child, exists := node.Children[segment]
		if !exists {
			return 0, false
		}
		node = child
	}
	if node.isEnd {
		return node.Value, true
	}
	return 0, false
}

func (t *Trie) Delete(key string) bool {
	segments := strings.Split(key, "/")
	return t.deleteHelper(t.Root, segments, 0)
}

func (t *Trie) deleteHelper(node *TrieNode, segments []string, depth int) bool {
	if node == nil {
		return false
	}

	// If we have reached the end of the key
	if depth == len(segments) {
		if !node.isEnd {
			return false // Key does not exist
		}
		node.isEnd = false // Unmark the end of the key
		node.Value = 0

		// If the node has no Children, it can be deleted
		return len(node.Children) == 0
	}

	segment := segments[depth]
	if t.deleteHelper(node.Children[segment], segments, depth+1) {
		delete(node.Children, segment)
		return !node.isEnd && len(node.Children) == 0
	}

	return false
}
