package azbfs

import (
	"time"
)

func (p Path) LastModifiedTime() time.Time {
	if p.LastModified == nil {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC1123, *p.LastModified)
	if err != nil {
		return time.Time{}
	}

	return t
}
