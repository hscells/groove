#!/usr/bin/env bash

mkdir -p tar2_testing_topics

for E_FILE in $(ls query_protocol_reachability/test_data/queries/original)
do
    for T_FILE in $(ls /Users/s4558151/Repositories/tar/2018-TAR/Task2/Testing/topics)
    do
        if [[ ${E_FILE} == ${T_FILE} ]]; then
            cp query_protocol_reachability/test_data/queries/original/${T_FILE} tar2_testing_topics
        fi
    done
done

