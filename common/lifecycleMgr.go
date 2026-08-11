package common

import (
	"context"
	"fmt"
)

// JobErrorHandler defines a simple interface for handling errors that occur
// during the job lifecycle.
//
// This interface allows callers to provide custom behavior for error handling,
// such as logging, reporting, or transforming error messages.
type JobErrorHandler interface {
	Error(string)
}

// JobUIHooks defines a set of function callbacks that control how job
// execution interacts with the user (e.g., prompting, logging, warnings,
// awaiting user approval).
//
// This is implemented as a struct of function fields rather than an interface,
// so that safe defaults can be provided. Callers can override only the 1–2
// callbacks they care about, without writing boilerplate implementations for
// all of them.
//
// Example:
//
//   h := NewJobUIHooks()
//   h.Warn = func(msg string) { fmt.Println("⚠️", msg) }
//   h.Prompt = func(msg string, d PromptDetails) ResponseOption {
//       return ResponseOption{<my code here>}
//   }
//
//   h.Info("this uses the default")
//   h.Warn("this uses the custom override")

type JobUIHooks struct {
	Prompt                   func(message string, details PromptDetails) ResponseOption
	Info                     func(string)
	Warn                     func(string)
	E2EAwaitAllowOpenFiles   func()
	E2EAwaitEnumerationStart func(context.Context)
}

func NewJobUIHooks() *JobUIHooks {
	return &JobUIHooks{
		Prompt: func(message string, details PromptDetails) ResponseOption {
			return EResponseOption.Default() // default: safe no-op
		},
		Info: func(msg string) {
			// default: no-op
		},
		Warn: func(msg string) {
			// default: no-op
		},
		E2EAwaitAllowOpenFiles: func() {
			// default: no-op
		},
		E2EAwaitEnumerationStart: func(context.Context) {
			// default: no-op
		},
	}
}

// TODO : (gapra) : Refactor these names, leaving them as-is for now to limit the scope of changes.
var lcm *JobUIHooks

// fallbackLcm is used when GetLifecycleMgr is called with allowFallback set to
// true and no handler has been installed globally via SetUIHooks. It outputs all
// job messages to stdout so library callers get useful feedback by default.
var fallbackLcm = &JobUIHooks{
	Prompt: func(message string, details PromptDetails) ResponseOption {
		return EResponseOption.Yes() // auto-answer in non-interactive mode
	},
	Info:  func(msg string) { fmt.Println(msg) },
	Warn:  func(msg string) { fmt.Println(msg) },
	E2EAwaitAllowOpenFiles: func() {
		// default: no-op
	},
	E2EAwaitEnumerationStart: func(context.Context) {
		// default: no-op
	},
}

func GetLifecycleMgr(allowFallback ...bool) *JobUIHooks {
	if lcm != nil {
		return lcm
	}
	if len(allowFallback) > 0 && allowFallback[0] {
		return fallbackLcm
	}
	return lcm
}

func SetUIHooks(hooks *JobUIHooks) {
	lcm = hooks
}

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
