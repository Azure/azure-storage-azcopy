package e2etest

import (
	"net/http"
	"net/url"
)

func CombineQuery(a, b url.Values) url.Values {
	out := make(url.Values)
	for k, v := range a {
		out[k] = append(out[k], v...)
	}
	for k, v := range b {
		out[k] = append(out[k], v...)
	}
	return out
}

// Ensure all types match interfaces
func init() {
	_ = []ARMSubject{&ARMClient{}, &ARMSubscription{}, &ARMResourceGroup{}, &ARMManagedDisk{}, &ARMStorageAccount{}}
}

type ARMClient struct {
	OAuth      AccessToken
	HttpClient *http.Client
}

func (c *ARMClient) CanonicalPath() string {
	return "/"
}

func (c *ARMClient) httpClient() *http.Client {
	if c.HttpClient != nil {
		return c.HttpClient
	}

	return http.DefaultClient
}

func (c *ARMClient) token() AccessToken {
	return c.OAuth
}

func (c *ARMClient) NewSubscriptionClient(subID string) *ARMSubscription {
	return &ARMSubscription{
		ParentSubject[*ARMClient]{c, c},
		subID,
	}
}
