package e2etest

import (
	"fmt"
	"reflect"
)

///*
//Initially, the intent was to opt for structs for each command (e.g. CopyParameters, SyncParameters),
//but upon further thought that does not align with the way the framework will be calling AzCopy
//(e.g. usually copy/sync multiple times in the same test)
//
//The goal of using structs to segregate the flags associated with different commands was to
//make it easier to interpret which flags are compatible with which verbs.
//
//In an attempt to try to alleviate that and provide a versatile solution,
//the verbs listed as constants below should be grouped by commonality.
//*/
//
//type AzCopyFlag struct {
//	Name           string
//	SupportedVerbs AzCopyVerb
//}
//
//var (
//	AzCopyFlagRecursive = AzCopyFlag{Name: "recursive", SupportedVerbs: AzCopyVerbCopy | AzCopyVerbSync | AzCopyVerbRemove}
//)

// MapFromTags Recursively builds a map[string]string from a reflect.val
func MapFromTags(val reflect.Value, tagName string) map[string]string {

	queue := []reflect.Value{val}
	out := make(map[string]string)

	for len(queue) != 0 {
		val := queue[0]
		queue = queue[1:]
		t := val.Type()
		numField := t.NumField()

		for i := 0; i < numField; i++ {
			key, ok := t.Field(i).Tag.Lookup(tagName)
			if ok {
				out[key] = fmt.Sprint(val.Field(i))
			} else if val.Field(i).Kind() == reflect.Struct {
				queue = append(queue, val.Field(i))
			}
		}
	}

	return out
}

// SampleFlags is a temporary stub to demonstrate how to create a flags struct
type SampleFlags struct {
	Recursive *bool `flag:"recursive"`
}
