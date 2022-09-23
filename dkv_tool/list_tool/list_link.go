package list_tool

import (
	"fmt"
	"sync"
)
import "errors"

type linkListNode[T any] struct {
	data T
	prev *linkListNode[T]
	next *linkListNode[T]
}

type DkvLinkList[T any] struct {
	size  int
	head  *linkListNode[T]
	tail  *linkListNode[T]
	mutex *sync.Mutex
}

func (l *DkvLinkList[T]) IsEmpty() bool {
	return l.size <= 0
}

func (l *DkvLinkList[T]) Size() int {
	return l.size
}

func (l *DkvLinkList[T]) Put(v T) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	node := &linkListNode[T]{data: v, prev: nil, next: nil}
	if l.tail == nil {
		l.size = 1
		l.head = node
		l.tail = node
	} else {
		l.size = l.size + 1
		l.tail.next = node
		node.prev = l.tail
		l.tail = node
	}
}

func (l *DkvLinkList[T]) Get(idx int) (T, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var defaultV T
	var node = l.head
	for i := 0; i < (idx - 1); i++ {
		if node != nil {
			node = node.next
		} else {
			return defaultV, errors.New(fmt.Sprintf("invalid index %d", idx))
		}
	}

	return node.data, nil
}

func CreateDkvLinkList[T any]() *DkvLinkList[T] {
	return &DkvLinkList[T]{size: 0, head: nil, tail: nil, mutex: &sync.Mutex{}}
}
