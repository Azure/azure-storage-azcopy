#!/usr/bin/env bash

# find and kill the ste
# no longer useful since azcopy doesn't run in the background mode any more
# the [] around 'a' is there so that grep doesn't find itself, it is a known trick
# kill $(ps aux | grep '[a]zcopy' | awk '{print $2}')

# remove logs and memory mapped plan files
rm -rf ~/.azcopy/*
