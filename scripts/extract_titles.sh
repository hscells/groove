#!/usr/bin/env bash

TOPICS=/Users/s4558151/Repositories/infs7410/2019/assessments/project/phase-1/tar/2017-TAR/testing/topics
OUTPUT=tar17_testing_titles

mkdir -p ${OUTPUT}

for FILE in $(ls ${TOPICS})
do
    ECHO ${FILE}
    TITLE=$(grep -E "Topic: CD[0-9]+" ${TOPICS}/${FILE} | cut -d" " -f2)
    grep "Title:" ${TOPICS}/${FILE} | cut -d" " -f2- > ${OUTPUT}/${TITLE}
done

echo "done!"