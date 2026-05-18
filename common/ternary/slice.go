package ternary

func FirstOrZero[T any](list []T) T {
	if len(list) != 0 {
		return list[0]
	}

	var zero T
	return zero
}
