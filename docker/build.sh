#!/bin/bash
echo "Using Go - $(go version)"
rm -rf azcopy
rm -rf azure-storage-azcopy
go build -o azcopy
