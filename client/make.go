package client

type MakeOptions struct {
	QuotaGB uint32
}

func (cc Client) Make(resource string, options MakeOptions) error {
	return nil
}
