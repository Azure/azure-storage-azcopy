// Copyright © 2025 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

func (l *LinkedList[T]) Front() T {
	return l.front.data
}

func (l *LinkedList[T]) Back() T {
	return l.back.data
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
