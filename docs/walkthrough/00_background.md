# Background

## Overview

Cherami is the mSCAPE orchestration module for pathogen pipelines that run downstream of sample ingest. To add a pipeline to Cherami you need a Nextflow pipeline and a Python module that defines how Cherami runs the pipeline.

Cherami provides:
- The RabbitMQ lifecycle of consuming, acking and nacking messages through Varys.
- Creating, polling and cleaning up Kubernetes jobs.
- Retries and timeouts.
- Version checks against Onyx for incoming samples.

Each Cherami process (started with `cherami serve <config.json>`) runs one worker for one pipeline, so each pipeline needs its own config file and its own deployment.

## Message lifecycle

*diagram here*

## Development installation

The recommended way of installing this repo is using uv:

```bash
git clone https://github.com/ukhsa-collaboration/cherami
cd cherami
uv run pre-commit install
uv run pytest
```

However other methods (such as conda or venv) will work:

```bash
git clone https://github.com/ukhsa-collaboration/cherami
cd cherami
conda create -n cherami python=3.12 "pip>=25.1"
conda activate cherami
pip install --group dev
pre-commit install
pytest
```
