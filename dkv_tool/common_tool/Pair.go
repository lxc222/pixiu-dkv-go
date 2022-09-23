package common_tool

type Pair[F any, S any] struct {
	Left  F
	Right S
}

func CreatePair[F any, S any](left F, right S) *Pair[F, S] {
	return &Pair[F, S]{
		left, right,
	}
}
