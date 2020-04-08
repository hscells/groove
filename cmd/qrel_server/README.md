# About `qrel_server`

`qrel_server` is a tool for streaming qrels via RPC. For very large qrels files, it can reduce the time needed to load qrels files into memory.

```
Usage: qrel_server QRELSFILE

Positional arguments:
  QRELSFILE              path to qrels file to host

Options:
  --help, -h             display this help and exit
  --version              display version and exit
```