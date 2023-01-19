package e2etest

// todo: upgrade to go 1.18 and use generics
func BoolPointer(b bool) *bool {
	return &b
}
