# Adding a new pipeline

This guide describes how to add a new pipeline end-to-end which covers writing a new config file, implementing a new `Pipeline` class, and either using the base `Worker` (or extending it when needed).

## 1. Overview
Broadly
- A `Pipeline` encapsulates everything Cherami needs to execute a Nextflow workflow for a sample:
  - how to generate a samplesheet
  - decision logic (defined in `should_run`) for skipping samples
  - trace file evaluation rules (`proc_names`)
- A `Worker` encapsulates the RabbitMQ/Varys orchestration:
  - receives messages from an exchange/queue
  - extracts `climb_id` and a `match_uuid` job identifier from an incoming message payload
  - calls `pipeline.should_run(...)` and either skips or launches the pipeline via `PipelineRunner`
  - optionally republishes the payload to a downstream queue on success

A single JSON config file describes configures both the worker and pipeline objects for a single characterisation pipeline.

## 2. Create a new config file

Create a new config file under `configs/`, for example `configs/cherami_my_pipeline.json`. Use `configs/cherami_amr.json` or `configs/cherami_orange_box.json` as a template and edit values.

Important naming rules:
- Pipeline `name` must not contain underscores (`_`), because it is used in Kubernetes Job names which forbid them.
  - Pipeline `name` is hyphenated (e.g. `my-pipeline`), but the Python module name is underscored (e.g. `my_pipeline.py`).

Minimal structure:

```json
{
  "global": {
    "work_dir": "/shared/team/projects/downstream_orchestration/work",
    "output_dir": "/shared/team/projects/downstream_orchestration/output"
  },
  "pipeline": {
    "name": "my-pipeline",
    "version": "0.1.0",
    "path": "/shared/team/projects/downstream_orchestration/my-pipeline-repo",
    "cpus": 4,
    "mem": "8G",
    "cpu_limit": 4,
    "mem_limit": "8G",
    "nf_config_path": "/shared/team/projects/downstream_orchestration/my-pipeline-repo/nextflow.config",
    "nf_profiles": ["docker"],
    "nf_extra_args": [],
    "namespace": "ns-synthscape-ukhsa",
    "container": "quay.io/climb-tre/nextflow",
    "backoff_limit": 5,
    "max_retries": 1,
    "retry_timeout": 10,
    "job_timeout": 3600
  },
  "worker": {
    "listen_exchange": "cherami_test",
    "listen_queue_suffix": "my_pipeline_queue",
    "publish_queue_suffix": null,
    "publish_exchange": null,
    "varys_config_path": "./conf/varys.cfg",
    "varys_log_path": "./my_pipeline_varys.log"
  }
}
```

Field-by-field documentation can be found in `docs/dev_config.md`.

## 3. Implement the pipeline module

Pipelines live under `src/cherami/pipelines/`. 

Create `src/cherami/pipelines/my_pipeline.py`.

This module at minimum needs:
- a `Pipeline` subclass
- a `build_worker(...)` function that constructs the pipeline and returns a `Worker` object.
- a `build_pipeline(...)` function that constructs the pipeline and returns a `Pipeline` object.

Example using the base `Worker` class:

```python
from pathlib import Path

from cherami.config import PipelineConfig, WorkerConfig
from cherami.pipelines.pipeline import Pipeline
from cherami.pipelines.worker import Worker


class MyPipeline(Pipeline):
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        ...


def build_pipeline(pipeline_config: PipelineConfig) -> Pipeline:
    return MyPipeline(pipeline_config)


def build_worker(
    worker_config: WorkerConfig,
    pipeline_config: PipelineConfig,
    work_dir: Path,
    output_dir: Path,
) -> Worker:
    pipeline = build_pipeline(pipeline_config)
    return Worker(worker_config, pipeline, work_dir, output_dir)
```

Notes:
- `samples` is currently treated as a list of one ID, but is intentionally a list for future batching.
- `job_id` comes from the upstream message payload (`match_uuid`) and is used for Kubernetes job naming.
- The samplesheet format is pipeline-specific - it must match what your Nextflow pipeline expects.

## 4. Optional: pipeline decision logic and evaluation

### 4.1. `should_run`

Override `Pipeline.should_run(sample_id)` to skip samples before launching Kubernetes jobs.

When this returns `False`, the worker calls `Worker.on_skip(...)` and acknowledges the message.

This would typically contain onyx queries and return True/False based on these.

### 4.2. `proc_names`

Override `Pipeline.proc_names` to define allowed Nextflow process exit codes when evaluating the trace file.

If `proc_names` is empty, the default behaviour is: every process must exit with code `0`.

### 4.3. `validate`

Override `Pipeline.validate()` to add pipeline-specific checks that run before a pipeline is executed.

## 5. Using the base worker

The default `Worker` implementation in `src/cherami/pipelines/worker.py`:
- Listens to an exchange
- expects message payloads containing `climb_id` and `match_uuid`
- can optionally republish successful payloads to a downstream queue when `publish_queue_suffix` is configured

If that is all you need, do not create a custom worker class: just return the base `Worker` from your module `build_worker(...)`.

## 6. When to extend the base worker (and how)

Extend the base worker when you need pipeline-specific orchestration behaviour, for example:
- a different payload (not `climb_id`/`match_uuid`)
- you want to publish to additional queues on success
- you want custom retry behaviour (e.g. dead-letter, alerts etc etc)
- you want special handling for “skipped” samples

In this case you can create a subclass and override those event hooks:
- `on_skip(message)`
- `on_success(message, payload)`
- `on_retry(message)`
- `on_sample_failure(message)`

Then return your custom worker from `build_worker(...)`.

## 7. Running and verifying

The normal entrypoint is:

```bash
uv run cherami serve path/to/config.json
```

To print a json representation of the message queue linkage for a config:

```bash
uv run cherami describe path/to/config.json
```

To evaluate `should_run(sample_id)` without needing to spawn workers use:

```bash
uv run cherami evaluate path/to/config.json SAMPLE_1 SAMPLE_2
```
