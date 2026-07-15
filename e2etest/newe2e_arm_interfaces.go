package e2etest

import (
	"net/http"
	"net/url"
)

type ARMSubject interface {
	CanonicalPath() string

	token() AccessToken
	httpClient() *http.Client
}

type ARMCustomManagementURI interface {
	ARMSubject

	managementURI() url.URL
}

type ParentSubject[T ARMSubject] struct {
	parent T
	root   *ARMClient
}

func (p ParentSubject[T]) CanonicalPath() string {
	return p.parent.CanonicalPath()
}

func (p ParentSubject[T]) token() AccessToken {
	return p.root.token()
}

func (p ParentSubject[T]) httpClient() *http.Client {
	return p.root.httpClient()
}

type ARMRequestPreparer interface {
	PrepareRequest(settings *ARMRequestSettings)
}
