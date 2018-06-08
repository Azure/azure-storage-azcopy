#!/usr/bin/env bash

export GOOS=darwin
go build -o azcopy

export GOOS=linux
go build -o azcopy-linux

export GOOS=windows
go build -o azcopy-windows.exe