#!/bin/bash

# this script provides a quick way to validate whether a change causes a performance gain versus regression

RunCurrent () {
  ./azcopy_current cp 'src' 'dst' --recursive --log-level=WARNING --check-length=false
}

RunNew () {
  ./azcopy_new cp 'src' 'dst' --recursive --log-level=WARNING --check-length=false
}

export AZCOPY_LOG_LOCATION=/datadrive/logs
export AZCOPY_JOB_PLAN_LOCATION=/datadrive/plans

for i in {1..5}
do
  # start with the new version, which might give it a disadvantage
  # but since we run the experiment repeately, the results average out eventually
  # in addition, it's better to give the disadvantage to the new version so that we can be extra sure that it's better/equal to current
  echo Running new version for "$i"th time >> cmd-output.txt

  # keep the result of the run, in case any error occurs
  start_time=$(date +%s)
  RunNew >> cmd-output.txt
  end_time=$(date +%s)

  # insert a record into the result CSV file
  # this format is used so that we can import it easily into Excel
  echo $i, new, $(expr "$end_time" - "$start_time") >> result.csv

  # run the current version immedietely after
  echo Running current version for "$i"th time >> cmd-output.txt

  # do the same for current version
  start_time=$(date +%s)
  RunCurrent >> cmd-output.txt
  end_time=$(date +%s)
  echo $i, current, $(expr "$end_time" - "$start_time") >> result.csv
done