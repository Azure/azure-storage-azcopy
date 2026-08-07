#!/bin/bash

set -e

# Run tests, write coverage data to a file, and pipe output to a file
echo "Running tests in ${directory}. Writing coverage data to ${coverageReportName}.txt and test results to ${testReportName}.txt."
go test -timeout=1h -v -coverprofile="${coverageReportName}.txt" "./${directory}" | tee "${testReportName}.txt"

exit_code=${PIPESTATUS[0]} # PIPESTATUS is defined by Bash, so this script must be run with Bash, not sh.

# Generate the jUnit report
echo "Generating jUnit report ${testReportName}.xml"
cat "${testReportName}.txt" | $(go env GOPATH)/bin/go-junit-report > "${testReportName}.xml"

# Convert coverage data to JSON format
echo "Generating JSON coverage report ${coverageReportName}.json"
$(go env GOPATH)/bin/gocov convert "${coverageReportName}.txt" > "${coverageReportName}.json"

# Convert coverage data to XML format
echo "Generating XML coverage report ${coverageReportName}.xml"
$(go env GOPATH)/bin/gocov-xml < "${coverageReportName}.json" > "${coverageReportName}.xml"

exit "$exit_code"
