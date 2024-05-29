package e2etest

func init() {
	suiteManager.RegisterSuite(&S2SSuite{})
}

type S2SSuite struct{}
