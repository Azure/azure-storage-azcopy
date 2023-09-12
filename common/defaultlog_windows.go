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

package common

import (
	"os"
	"syscall"
	"unsafe"
)

// forceLog should rarely be used. It forceable logs an entry to the
// Windows Event Log (on Windows) or to the SysLog (on Linux)
func forceLog(level LogLevel, msg string) {
	var el eventType
	switch level {
	case LogError, LogFatal, LogPanic:
		el = elError
	case LogWarning:
		el = elWarning
	case LogInfo:
		el = elInfo
	}
	// We are logging it, ensure trailing newline
	if len(msg) == 0 || msg[len(msg)-1] != '\n' {
		msg += "\n" // Ensure trailing newline
	}
	reportEvent(el, 0, msg)
}

type eventType int16

const (
	elSuccess eventType = 0
	elError   eventType = 1
	elWarning eventType = 2
	elInfo    eventType = 4
)

var reportEvent = func() func(eventType eventType, eventID int32, msg string) {
	advAPI32 := syscall.MustLoadDLL("advapi32.dll") // lower case to tie in with Go's sysdll registration
	registerEventSource := advAPI32.MustFindProc("RegisterEventSourceW")

	sourceName, _ := os.Executable()
	sourceNameUTF16, _ := syscall.UTF16PtrFromString(sourceName)
	handle, _, lastErr := registerEventSource.Call(uintptr(0), uintptr(unsafe.Pointer(sourceNameUTF16)))
	if lastErr == nil { // On error, logging is a no-op
		return func(eventType eventType, eventID int32, msg string) {}
	}
	reportEvent := advAPI32.MustFindProc("ReportEventW")
	return func(eventType eventType, eventID int32, msg string) {
		s, _ := syscall.UTF16PtrFromString(msg)
		_, _, _ = reportEvent.Call(
			uintptr(handle),             // HANDLE  hEventLog
			uintptr(eventType),          // WORD    wType
			uintptr(0),                  // WORD    wCategory
			uintptr(eventID),            // DWORD   dwEventID
			uintptr(0),                  // PSID    lpUserSid
			uintptr(1),                  // WORD    wNumStrings
			uintptr(0),                  // DWORD   dwDataSize
			uintptr(unsafe.Pointer(&s)), // LPCTSTR *lpStrings
			uintptr(0))                  // LPVOID  lpRawData
	}
}()
