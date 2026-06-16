[![Docker](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml/badge.svg)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/docker-build.yml)
[![Tests](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ukhsa-collaboration/cherami/actions/workflows/ci.yml)

# cherami

cherami is the mSCAPE orchestration module for pathogen pipelines, designed to run any workflow that occurs downstream of sample ingest. Its goal is to make pipeline orchestration straightforward to implement and operate across a diverse range of pipelines, by providing a standard method to integrate pipelines with Kubernetes for execution and RabbitMQ for orchestration.

## I am planning to develop a nextflow pipeline that will be used in cherami in the future

Read [this](docs/dev_nextflow_pipeline_requirements.md) first.

## I have a nextflow pipeline already developed that I want to add to cherami

Read [this](docs/dev_adding_new_pipeline.md) and then [this](docs/dev_k8_deployment_of_cherami.md).

## Installation

### Containers
- Containers are built for each tagged release, and pulished to GHCR:
  ```
  docker pull ghcr.io/ukhsa-collaboration/cherami:latest
  ```
- Containers are also built off of the dev branch for testing purposes
  ```
  docker pull ghcr.io/ukhsa-collaboration/cherami:dev
  ```

### Local
- Alternatively you can install into a local environment
  ```
  pip install git+https://github.com/ukhsa-collaboration/cherami.git
  ```

## Usage

```
Usage: cherami [OPTIONS] COMMAND [ARGS]...

Commands:
  serve
  describe
  evaluate
```

Global options:
```
--log <PATH> - log file path; logs to stderr when omitted
--log-level <DEBUG|INFO|WARNING|ERROR> - default INFO
```
## Sub-commands

### serve
Main entrypoint. Launch the worker defined by a config file.

```
cherami serve <config>
```

## Configuration

cherami requires a JSON configuration file passed as a positional argument to `serve`. This file configures the pipeline and worker for execution.

Further documentation for the config file can be found [here](docs/dev_config.md).

## Development

### Implementation

Cherami uses 2 systems, Kubernetes for job execution and RabbitMQ for sample orchestration.

RabbitMQ is used as the message queue system to manage samples as they move through pipelines. Cherami deploys Workers that are configured to listen to queues, decide whether to launch (or skip) pipelines based on pipeline-specific decision logic. They can also publish messages to new queues to chain pipelines together. For an overview see [rmq_implementation](docs/img/RMQ_implementation.png)

Kubernetes is used as the execution platform for pipelines. For each incoming sample, cherami creates a Kubernetes Job that wraps the appropriate Nextflow pipeline, submits it to the cluster, and monitors its status until completion.

### For developers

For development notes please refer to [docs/README.md](docs/README.md)
