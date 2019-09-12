#!/usr/bin/env bash

TOPICS=/Users/s4558151/Repositories/infs7410/2019/assessments/project/phase-1/tar/2018-TAR/training/topics
OUTPUT=tar2_training_pmids

mkdir -p ${OUTPUT}

for FILE in $(ls ${TOPICS})
do
    ECHO ${FILE}
    TITLE=$(grep -E "Topic: CD[0-9]+" ${TOPICS}/${FILE} | cut -d" " -f2)
    grep -E "    [0-9]+" ${TOPICS}/${FILE} | cut -d" " -f5 > ${OUTPUT}/${TITLE}
done

echo "done!"