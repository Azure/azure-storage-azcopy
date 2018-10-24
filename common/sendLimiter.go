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
	"context"
	"golang.org/x/sync/semaphore"
)

type SendLimiter interface {
	AcquireSendSlot(ctx context.Context) error
	ReleaseSendSlot()
}

type sendLimiter struct {
	semaphore *semaphore.Weighted
}

func NewSendLimiter(maxConcurrentSends int64) SendLimiter {
	return &sendLimiter {
		semaphore: semaphore.NewWeighted(maxConcurrentSends)}
	}

func (s *sendLimiter) AcquireSendSlot(ctx context.Context) error{
	return s.semaphore.Acquire(ctx, 1)
}

func (s *sendLimiter) ReleaseSendSlot() {
	s.semaphore.Release(1)
}

