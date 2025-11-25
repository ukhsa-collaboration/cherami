[![Docker](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml/badge.svg)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml)
[![Tests](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml)

# cherami

cherami is the mSCAPE orchestration module for pathogen pipelines.

## Usage

```
Usage: cherami [OPTIONS] COMMAND [ARGS]...

Commands:
  describe
  evaluate
  run
  spawn
```

Global options:
```
--sample-log <PATH> - File path for per-pipeline results (default ./sample_log.jsonl)
--log <PATH> - log file path; logs to stderr when omitted
--log-level <DEBUG|INFO|WARNING|ERROR> - default INFO
```

## Sub-commands

### spawn
Main entrypoint. Launch one or more workers that listen for queue messages and run pipelines.

```
cherami --log worker.log spawn [WORKER_NAMES...]
```

`WORKER_NAMES` comes from the registered workers in `cherami.workers.WORKERS`. If no names are provided, all workers are started. Each worker runs in its own process.

### run
Run one or more pipelines directly against provided sample IDs.

```
cherami run SAMPLE_ID... --pipelines PIPELINE1,PIPELINE2
```

- `SAMPLE_ID...` - one or more sample identifiers.
- `--pipelines` - comma-separated pipeline names from `cherami.pipelines.PIPELINES`. When omitted, all pipelines are attempted.
The command will attempt to run a pipeline in the same manor as if recived from the message queue, and so is useful for debugging.

### describe
Print rabbitmq bindings.

```
cherami describe [WORKER_NAMES...]
```

Shows listen exchange/queue and publish exchange/queue for the selected workers (or all workers when none are specified).

### evaluate
Check whether pipelines would run for given samples without launching jobs.

```
cherami evaluate SAMPLE_ID... [--pipelines PIPELINE1,PIPELINE2]
```

Emits a tab-separated line per sample/pipeline with the `should_run` decision. Useful for dry-run validation of pipelines

## Installation

### Container (recommended)
- Pull the published image from GHCR:
  ```
  docker pull ghcr.io/ukhsa-collaboration/cherami:main
  ```
- For pre-release testing, use:
  ```
  docker pull ghcr.io/ukhsa-collaboration/cherami:pre-release
  ```

### Local
  ```
  pip install git+https://github.com/ukhsa-collaboration/cherami.git
  ```


## Development

For development notes please refer to [dev.md](docs/dev.md)
