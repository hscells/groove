#!/usr/bin/env bash

#template rank_clm $0
#template rank_clf $1
#template query_expansion $2
#template score_pubmed $3
#template only_score_pubmed $4
#template retrieval_model $5
#template clf_variations $6
#template pmids $7
#template titles $8s
#template cutoff $9
#template query_path $10
#template run_name $11
#template qrels $12

RANK_CLM=$1
RANK_CLF=$2
QUERY_EXPANSION=$3
SCORE_PUBMED=$4
ONLY_SCORE_PUBMED=$5
RETRIEVAL_MODEL=$6
CLF_VARIATIONS=$7
PMIDS=$8
TITLES=$9
CUTOFF=${10}
QUERY_PATH=${13}
RUN_NAME=${11}
QRELS=${12}
BOOGIE=/Users/s4558151/go/src/github.com/hscells/boogie/cmd/boogie/main.go

go run ${BOOGIE} --pipeline clf.json ${RANK_CLM} ${RANK_CLF} ${QUERY_EXPANSION} ${SCORE_PUBMED} ${ONLY_SCORE_PUBMED} ${RETRIEVAL_MODEL} ${CLF_VARIATIONS} ${PMIDS} ${TITLES} ${CUTOFF} ${QUERY_PATH} ${RUN_NAME} ${QRELS}
