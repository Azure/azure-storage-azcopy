package e2etest

func GetTypeOrZero[T any](in any) (out T) {
	if out, ok := in.(T); ok {
		return out
	}

	return
}

func DerefOrZero[T any](in *T) (out T) {
	if in != nil {
		out = *in
	}

	return
}

func PtrOf[T any](in T) (out *T) {
	return &in
}

func IsZero[T comparable](in T) bool {
	var zero T
	return in == zero
}
