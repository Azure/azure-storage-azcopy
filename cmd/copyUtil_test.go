// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestUrlIsContainerOrBlob(t *testing.T) {
	a := assert.New(t)
	util := copyHandlerUtil{}

	testUrl := url.URL{Path: "/container/dir1"}
	isContainer := util.urlIsContainerOrVirtualDirectory(&testUrl)
	a.False(isContainer)

	testUrl.Path = "/container/dir1/dir2"
	isContainer = util.urlIsContainerOrVirtualDirectory(&testUrl)
	a.False(isContainer)

	testUrl.Path = "/container/"
	isContainer = util.urlIsContainerOrVirtualDirectory(&testUrl)
	a.True(isContainer)

	testUrl.Path = "/container"
	isContainer = util.urlIsContainerOrVirtualDirectory(&testUrl)
	a.True(isContainer)

	// root container
	testUrl.Path = "/"
	isContainer = util.urlIsContainerOrVirtualDirectory(&testUrl)
	a.True(isContainer)
}

func TestIPIsContainerOrBlob(t *testing.T) {
	a := assert.New(t)
	util := copyHandlerUtil{}

	testIP := url.URL{Host: "127.0.0.1:8256", Path: "/account/container"}
	testURL := url.URL{Path: "/account/container"}
	isContainerIP := util.urlIsContainerOrVirtualDirectory(&testIP)
	isContainerURL := util.urlIsContainerOrVirtualDirectory(&testURL)
	a.True(isContainerIP)   // IP endpoints contain the account in the path, making the container the second entry
	a.False(isContainerURL) // URL endpoints do not contain the account in the path, making the container the first entry.

	testURL.Path = "/account/container/folder"
	testIP.Path = "/account/container/folder"
	isContainerIP = util.urlIsContainerOrVirtualDirectory(&testIP)
	isContainerURL = util.urlIsContainerOrVirtualDirectory(&testURL)
	a.False(isContainerIP)   // IP endpoints contain the account in the path, making the container the second entry
	a.False(isContainerURL) // URL endpoints do not contain the account in the path, making the container the first entry.

	testURL.Path = "/account/container/folder/"
	testIP.Path = "/account/container/folder/"
	isContainerIP = util.urlIsContainerOrVirtualDirectory(&testIP)
	isContainerURL = util.urlIsContainerOrVirtualDirectory(&testURL)
	a.True(isContainerIP)  // IP endpoints contain the account in the path, making the container the second entry
	a.True(isContainerURL) // URL endpoints do not contain the account in the path, making the container the first entry.
	// The behaviour isn't too different from here.
}