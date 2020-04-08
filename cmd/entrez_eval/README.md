# About `entrez_eval`

`entrez_eval` is a tool for the evaluation of TREC run files using qrels.

```
Usage: entrez_eval [--relevancegrade RELEVANCEGRADE] [--evaluation EVALUATION] [--resulthandlers RESULTHANDLERS] [--runoutput RUNOUTPUT] [--evaluationoutput EVALUATIONOUTPUT] [--summary] [--topic TOPIC] [--estimaten ESTIMATEN] QRELSFILE RUNFILE

Positional arguments:
  QRELSFILE              Path to qrels file
  RUNFILE                Path to run file

Options:
  --relevancegrade RELEVANCEGRADE, -l RELEVANCEGRADE
                         Minimum level of relevance to consider
  --evaluation EVALUATION, -e EVALUATION
                         Which evaluation measures to use
  --resulthandlers RESULTHANDLERS, -r RESULTHANDLERS
                         Which run handlers to use
  --runoutput RUNOUTPUT, -o RUNOUTPUT
                         Name of processed run file
  --evaluationoutput EVALUATIONOUTPUT, -q EVALUATIONOUTPUT
                         Name of results file
  --summary, -s          Only output summary information
  --topic TOPIC, -t TOPIC
                         Topic to evaluate (only when loading qrels using RPC)
  --estimaten ESTIMATEN, -n ESTIMATEN
                         Estimate number of documents
  --help, -h             display this help and exit
  --version              display version and exit
```