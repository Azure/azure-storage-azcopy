package ternary

func DerefOrZero[T any](in *T) (out T) {
	if in != nil {
		out = *in
	}

	return
}

func DefaultValue[T any](in *T, defaultValue T) *T {
	return Iff(in == nil, &defaultValue, in)
}
