package ternary

func DerefOrZero[T any](in *T) (out T) {
	if in != nil {
		out = *in
	}

	return
}
