#!/usr/bin/env bash

# find and kill the ste
# the [] around 'a' is there so that grep doesn't find itself, it is a known trick
# kill $(ps aux | grep '[a]zcopy' | awk '{print $2}')

# remove logs and memory mapped files
rm *.log
rm ~/.azcopy/*.steV*
