package cmd

import "path"

type excludeFilter struct {
	pattern string
}

func (f *excludeFilter) pass(entity genericEntity) bool {
	matched, err := path.Match(f.pattern, entity.name)

	// if the pattern failed to match with an error, then we assume the pattern is invalid
	// and let it pass
	if err != nil {
		return true
	}

	if matched {
		return false
	}

	return true
}

func buildExcludeFilters(patterns []string) []entityFilter {
	filters := make([]entityFilter, 0)
	for _, pattern := range patterns {
		filters = append(filters, &excludeFilter{pattern: pattern})
	}

	return filters
}

// design explanation:
// include filters are different from the exclude ones, which work together in the "AND" manner
// meaning and if an entity is rejected by any of the exclude filters, then it is rejected by all of them
// as a result, the exclude filters can be in their own struct, and work correctly
// on the other hand, include filters work in the "OR" manner
// meaning that if an entity is accepted by any of the include filters, then it is accepted by all of them
// consequently, all the include patterns must be stored together
type includeFilter struct {
	patterns []string
}

func (f *includeFilter) pass(entity genericEntity) bool {
	if len(f.patterns) == 0 {
		return true
	}

	for _, pattern := range f.patterns {
		matched, err := path.Match(pattern, entity.name)

		// if the pattern failed to match with an error, then we assume the pattern is invalid
		// and ignore it
		if err != nil {
			continue
		}

		// if an entity is accepted by any of the include filters
		// it is accepted
		if matched {
			return true
		}
	}

	return false
}

func buildIncludeFilters(patterns []string) []entityFilter {
	return []entityFilter{&includeFilter{patterns: patterns}}
}
