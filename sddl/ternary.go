package sddl

// TODO: Consider moving this to azcopy's iff functions
//       Maybe not, since this is intended to grow into its own library.
func ternaryInt(test bool, trueVal, falseVal int) int {
	if test {
		return trueVal
	} else {
		return falseVal
	}
}
