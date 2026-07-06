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

package common

import (
	"fmt"
	"os"
)

type EnvironmentVariable struct {
	Name         string
	DefaultValue string
	Description  string
	Hidden       bool
}

// GetEnvironmentVariable gets the environment variable or its default value
func GetEnvironmentVariable(env EnvironmentVariable) string {
	value := os.Getenv(env.Name)
	if value == "" {
		return env.DefaultValue
	}
	return value
}

// ClearEnvironmentVariable clears the environment variable
func ClearEnvironmentVariable(variable EnvironmentVariable) {
	_ = os.Setenv(variable.Name, "")
}

// SetInstanceDiscovery sets the instance discovery flag based on the passed in value else defaults to false.
var IsDiscoveryDisabled bool = false

func SetInstanceDiscovery(instanceDiscovery bool) {
	if instanceDiscovery == true { //if hidden env variable has been set to false, we want to turn on
		IsDiscoveryDisabled = true
		fmt.Println("Instance discovery is disabled")
	}
}
