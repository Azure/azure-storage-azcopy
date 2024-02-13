package e2etest

import "strings"

type PathTrie[T any] struct {
	Nodes *TrieNode[T]
	Sep   rune
}

type TrieNode[T any] struct {
	Parent   *TrieNode[T]
	Children map[string]*TrieNode[T]
	Segment  string

	Data *T
}

func NewTrie[T any](Separator rune) *PathTrie[T] {
	return &PathTrie[T]{Sep: Separator, Nodes: createNode[T]("", nil)}
}

func createNode[T any](Segment string, Parent *TrieNode[T]) *TrieNode[T] {
	return &TrieNode[T]{Segment: Segment, Parent: Parent, Children: make(map[string]*TrieNode[T])}
}

func (t *PathTrie[T]) walk(path string, doCreate bool) *TrieNode[T] {
	currentNode := t.Nodes
	for len(path) > 0 {
		segmentLength := strings.IndexRune(path, t.Sep)
		if segmentLength == -1 {
			segmentLength = len(path)
		}

		segment := path[:segmentLength]
		newNode, OK := currentNode.Children[segment]
		if !OK {
			if doCreate {
				newNode = createNode(segment, currentNode)
				currentNode.Children[segment] = newNode
			} else {
				return nil
			}
		}

		currentNode = newNode
		if segmentLength == len(path) {
			path = ""
		} else {
			path = path[segmentLength+1:]
		}
	}

	return currentNode
}

func (t *PathTrie[T]) Insert(path string, data *T) {
	newNode := t.walk(path, true)
	newNode.Data = data
}

func (t *PathTrie[T]) Get(path string) *T {
	node := t.walk(path, false)
	if node == nil {
		return nil
	}

	return node.Data
}

func (t *PathTrie[T]) Remove(path string) {
	node := t.walk(path, false)

	for node.Parent != nil && node.Data == nil {
		childSegment := node.Segment
		node = node.Parent

		delete(node.Children, childSegment)
	}
}

type TraversalOperation uint8

const (
	// TraversalOperationContinue continue traversing children
	TraversalOperationContinue TraversalOperation = iota
	// TraversalOperationStop stop traversing children
	TraversalOperationStop
	// TraversalOperationRemove remove this node from the tree and all children
	TraversalOperationRemove
)

func (t *PathTrie[T]) Traverse(traversalFunc func(data *T) TraversalOperation) {
	t.Nodes.Traverse(traversalFunc)
}

func (n *TrieNode[T]) Traverse(traversalFunc func(data *T) TraversalOperation) {
	op := TraversalOperationContinue
	if n.Data != nil {
		op = traversalFunc(n.Data)
	}

	switch op {
	case TraversalOperationContinue:
		for _, v := range n.Children {
			v.Traverse(traversalFunc)
		}
	case TraversalOperationRemove:
		delete(n.Parent.Children, n.Segment)
	default:
	}
}
