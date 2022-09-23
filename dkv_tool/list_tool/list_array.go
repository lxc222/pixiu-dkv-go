package list_tool

import "errors"

type DkvArrayList[T any] struct {
	data []T
}

func (l *DkvArrayList[T]) IsEmpty() bool {
	return l.Size() <= 0
}

func (l *DkvArrayList[T]) Size() int {
	return len(l.data)
}

func (l *DkvArrayList[T]) Put(v T) {
	l.data = append(l.data, v)
}

func (l *DkvArrayList[T]) Get(idx int) (T, error) {
	var defaultV T
	if idx >= l.Size() {
		return defaultV, errors.New("index error")
	} else {
		return l.data[idx], nil
	}
}

func CreateDkvArrayList[T any]() *DkvArrayList[T] {
	return &DkvArrayList[T]{data: make([]T, 0, 10)}
}
