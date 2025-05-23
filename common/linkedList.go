package common

// Singly linked list intended for buckets for ste.RequestLifetimeTracker

type LinkedList[T any] struct {
	front *linkedListElement[T]
	back  *linkedListElement[T]
	len   int64
}

type linkedListElement[T any] struct {
	next *linkedListElement[T]
	data T
}

func (l *LinkedList[T]) Enum() *LinkedListEnumerator[T] {
	return &LinkedListEnumerator[T]{
		index:  0,
		target: l.back,
	}
}

func (l *LinkedList[T]) Len() int64 {
	return l.len
}

func (l *LinkedList[T]) PopRear() {
	if l.back == nil {
		return
	}

	l.back = l.back.next
	l.len--

	if l.back == nil {
		l.back = l.front
	}
}

func (l *LinkedList[T]) Insert(data T) {
	if l.front == nil {
		l.front = &linkedListElement[T]{
			data: data,
		}
		l.back = l.front
	} else {
		l.front.next = &linkedListElement[T]{
			data: data,
		}
		l.front = l.front.next
	}

	l.len++
}

type LinkedListEnumerator[T any] struct {
	index  int64
	target *linkedListElement[T]
}

func (l *LinkedListEnumerator[T]) HasData() bool {
	return l.target != nil
}

func (l *LinkedListEnumerator[T]) Next() {
	if l.target != nil {
		l.target = l.target.next
		l.index++
	}
}

func (l *LinkedListEnumerator[T]) Data() T {
	return l.target.data
}
