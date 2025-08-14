// Copyright © Microsoft <wastore@microsoft.com>
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

package ste

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// asks questions to confirm overwrite, one routine at a time, i.e. only one active question at a time
type overwritePrompter struct {
	lock *sync.Mutex

	// whether we should still ask the user for permission to overwrite the destination
	// if false, the user has already specified a "for all" answer
	shouldPromptUser map[common.EntityType]bool

	// if the user made a "for all" selection, save the response here
	savedResponse map[common.EntityType]bool
}

func (o *overwritePrompter) ShouldOverwrite(objectPath string, objectType common.EntityType) (shouldOverwrite bool) {
	// only one routine can ask the question or check the saved response at a time
	o.lock.Lock()
	defer o.lock.Unlock()

	if o.shouldPromptUser[objectType] {
		shouldOverwrite = o.promptForConfirmation(objectPath, objectType)
	} else {
		shouldOverwrite = o.savedResponse[objectType]
	}

	return
}

func (o *overwritePrompter) promptForConfirmation(objectPath string, objectType common.EntityType) (shouldOverwrite bool) {
	question := fmt.Sprintf("%s already exists at the destination. "+
		"Do you wish to overwrite?", objectPath)

	if objectType == common.EEntityType.Folder() {
		question = fmt.Sprintf("Folder %s already exists at the destination. "+
			"Do you wish to overwrite its properties?", objectPath)
	}

	answer := common.GetLifecycleMgr().OnPrompt(question,
		common.PromptDetails{
			PromptType:   common.EPromptType.Overwrite(),
			PromptTarget: objectPath,
			ResponseOptions: []common.ResponseOption{
				common.EResponseOption.Yes(),
				common.EResponseOption.No(),
				common.EResponseOption.YesForAll(),
				common.EResponseOption.NoForAll()},
		})

	switch answer {
	case common.EResponseOption.Yes():
		common.GetLifecycleMgr().OnInfo(fmt.Sprintf("Confirmed. %s will be overwritten.", objectPath))
		return true
	case common.EResponseOption.YesForAll():
		common.GetLifecycleMgr().OnInfo(fmt.Sprintf("Confirmed. All future %s conflicts will be overwritten.", strings.ToLower(objectType.String())))
		o.shouldPromptUser[objectType] = false
		o.savedResponse[objectType] = true
		return true
	case common.EResponseOption.No():
		common.GetLifecycleMgr().OnInfo(fmt.Sprintf("%s will be skipped", objectPath))
		return false
	case common.EResponseOption.NoForAll():
		name := strings.ToLower(objectType.String())
		if objectType == common.EEntityType.Folder() {
			name += " properties"
		}

		common.GetLifecycleMgr().OnInfo(fmt.Sprintf("No %s will be overwritten from now onwards.", name))
		o.shouldPromptUser[objectType] = false
		o.savedResponse[objectType] = false
		return false
	default:
		common.GetLifecycleMgr().OnInfo(fmt.Sprintf("Unrecognizable answer, skipping %s.", objectPath))
		return false
	}
}

func newOverwritePrompter() *overwritePrompter {
	return &overwritePrompter{
		lock: &sync.Mutex{},
		shouldPromptUser: map[common.EntityType]bool{
			common.EEntityType.Folder(): true,
			common.EEntityType.File():   true,
		},
		savedResponse: map[common.EntityType]bool{
			common.EEntityType.Folder(): false,
			common.EEntityType.File():   false,
		},
	}
}
