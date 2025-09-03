package traverser

// excludeContainerFilter filters out container names that must be excluded
type excludeContainerFilter struct {
	containerNamesList map[string]bool
}

func (s *excludeContainerFilter) DoesSupportThisOS() (msg string, supported bool) {
	return "", true
}

func (s *excludeContainerFilter) AppliesOnlyToFiles() bool {
	return false // excludeContainerFilter is related to container names, not related to files
}

func (s *excludeContainerFilter) DoesPass(storedObject StoredObject) bool {
	if len(s.containerNamesList) == 0 {
		return true
	}

	if _, exists := s.containerNamesList[storedObject.ContainerName]; exists {
		return false
	}
	return true
}

func buildExcludeContainerFilter(containerNames []string) []ObjectFilter {
	excludeContainerSet := make(map[string]bool)
	for _, name := range containerNames {
		excludeContainerSet[name] = true
	}

	return append(make([]ObjectFilter, 0), &excludeContainerFilter{containerNamesList: excludeContainerSet})
}

type FilterSet []ObjectFilter

// GetEnumerationPreFilter returns a prefix that is common to all the include filters, or "" if no such prefix can
// be found. (The implementation may return "" even in cases where such a prefix does exist, but in at least the simplest
// cases, it should return a non-empty prefix.)
// The result can be used to optimize enumeration, since anything without this prefix will fail the FilterSet
func (fs FilterSet) GetEnumerationPreFilter(recursive bool) string {
	if recursive {
		return ""
		// we don't/can't support recursive cases yet, with a strict prefix-based search.
		// Because if the filter is, say "a*", then, then a prefix of "a"
		// will find: enumerationroot/afoo and enumerationroot/abar
		// but it will not find: enumerationroot/virtualdir/afoo
		// even though we want --include-pattern to find that.
		// So, in recursive cases, we just don't use this prefix-based optimization.
		// TODO: consider whether we need to support some way to separately invoke prefix-based optimization
		//   and filtering.  E.g. by a directory-by-directory enumeration (with prefix only within directory),
		//   using the prefix feature in ListBlobs.
	}
	prefix := ""
	for _, f := range fs {
		if participatingFilter, ok := f.(PreFilterProvider); ok {
			// this filter knows how to participate in our scheme
			if prefix == "" {
				prefix = participatingFilter.GetEnumerationPreFilter()
			} else {
				// prefix already has a value, which means there must be two participating filters, and we can't handle that.
				// Normally this won't happen, because there's only one IncludeFilter on matter how many include Patterns have been supplied.
				return ""
			}
		}
	}
	return prefix
}
