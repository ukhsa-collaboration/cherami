[![Docker](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml/badge.svg)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml)
[![Tests](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml)

# cherami

cherami is the mSCAPE orchestration module for pathogen pipelines, designed to run any workflow that occurs downstream of sample ingest. Its goal is to make pipeline orchestration straightforward to implement and operate across a diverse range of pipelines, by providing standard templates that integrate pipelines with Kubernetes for execution and RabbitMQ for orchestration.

RabbitMQ is used as the message queue system to manage samples as they move through pipelines. Workers are configured to listen to queues, decide whether to launch or skip pipelines based on pipeline-specific logic, and can publish messages to new queues to chain pipelines together.

Kubernetes is used as the execution platform for pipelines. For each incoming sample that should run, cherami creates a Kubernetes Job that wraps the appropriate Nextflow pipeline, submits it to the cluster, and monitors its status until completion, applying per-pipeline settings such as resource limits, and custom samplesheet generation.

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
--config <PATH> - path to cherami JSON config (or set CHERAMI_CONFIG)
```

## Configuration

cherami reads a single JSON configuration file, provided via the `--config` option or the `CHERAMI_CONFIG` environment variable. This file configures both the available pipelines and the workers that will run those pipelines.

The `pipelines` section describes each pipeline, including its Nextflow configuration, working and output directories, and Kubernetes execution settings such as resource requests/limits, and retry behaviour. The `workers` section binds a worker name to a pipeline, and specifies the RabbitMQ exchanges, queue suffixes, and Varys configuration paths used to receive and publish messages.

Further documentation for the config file can be found here.

## Sub-commands

### spawn
Main entrypoint. Launch one or more workers that listen for queue messages and run pipelines.

```
cherami --log worker.log spawn [WORKER_NAMES...]
```

## Development

For development notes please refer to [dev.md](docs/dev.md)
