package common_tool

type Triple[F any, S any, T any] struct {
	Left   F
	Middle S
	Right  T
}

func CreateTriple[F any, S any, T any](left F, middle S, right T) *Triple[F, S, T] {
	return &Triple[F, S, T]{
		left, middle, right,
	}
}
