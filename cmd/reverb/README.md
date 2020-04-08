# About `reverb`

`reverb` is a tool for running concurrent groove/boogie pipelines by distributing the pipeline across (potentially remote) servers.

```
Usage: reverb [--pipeline PIPELINE] [--port PORT] [--hosts HOSTS] MODE [TEMPLATEARGS [TEMPLATEARGS ...]]

Positional arguments:
  MODE                   Mode to run reverb in [client/server]
  TEMPLATEARGS           Additional arguments to pass to experimental pipeline file

Options:
  --pipeline PIPELINE    Path to boogie experimental pipeline file
  --port PORT, -p PORT   Port to run server on
  --hosts HOSTS, -s HOSTS
                         When in client mode, list of reverb servers to distribute the pipeline across
  --help, -h             display this help and exit
  --version              display version and exit
```