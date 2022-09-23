package list_tool

type DkvList[T any] interface {
	IsEmpty() bool
	Size() int
	Put(v T)
	Get(idx int) (T, error)
}
