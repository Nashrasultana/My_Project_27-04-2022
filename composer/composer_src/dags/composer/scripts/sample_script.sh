#!/bin/usr/env bash

while read -r line;
do
echo "$line \n"
done < $(gsutil ls gs://europe-west2-composer-dev-t-2d8ce3b5-bucket/data/dag_variables| awk '{print $NF}')
