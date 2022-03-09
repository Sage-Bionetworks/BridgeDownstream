#! /bin/bash

set -ex

jobs=($(aws glue get-jobs | jq -r  '.Jobs[] | select(.Name | startswith("bridge-downstream-")) | .Name'))

for job in "${jobs[@]}"
do
  echo $job
  aws glue get-job-bookmark --job-name $job
done

set +ex
