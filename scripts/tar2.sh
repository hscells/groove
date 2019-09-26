#!/usr/bin/env bash

mkdir -p tar17_training_topics

for E_FILE in $(ls query_protocol_reachability/test_data/queries/original)
do
    for T_FILE in $(ls tar17_training_titles)
    do
        if [[ ${E_FILE} == ${T_FILE} ]]; then
            cp query_protocol_reachability/test_data/queries/original/${T_FILE} tar17_training_topics
        fi
    done
done

