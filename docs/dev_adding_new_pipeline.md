# Adding a new characterisation pipeline

This guide details the end-to-end process of adding a new characterisation pipeline to Cherami. This involves creating a configuration file, implementing a `Pipeline` class, and determining the appropriate `Worker` strategy.

## 1. Overview

Cherami manages characterisation pipelines using two key components:

1. **`Pipeline`**: Defines the logic and compute criteria for a specific workflow (e.g., AMR, Orange Box).
   - Generates the input samplesheet for Nextflow.
   - Implements decision logic to skip ineligible samples.
   - Validates execution success via a trace file produced by nextflow.

2. **`Worker`**: Manages orchestration.
   - Listens to RabbitMQ/Varys queues for messages from upstream pipelines.
   - Invokes `Pipeline` objects to execute jobs.
   - Handles the lifecycle of a sample via events e.g success, retries, skipping, and downstream publishing.

A single JSON configuration file specifies most of the config options, defining resources, container settings, exchange names etc. This configuration file is used to run a worker listening to the configured exchange via `cherami serve <config>`

## 2. Configuration

Create a new configuration file in `configs/` (e.g., `configs/cherami_my_pipeline.json`). Use existing configs like `configs/cherami_amr.json` as a templates.

### Naming Conventions
* **Pipeline Name**: Must be hyphenated (e.g., `my-pipeline`) because it is used to generate Kubernetes job names which can NOT contain underscores.
* **Module Name**: Can be underscored (e.g., `my_pipeline.py`).

### Minimal Structure

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
    "nf_profiles": [],
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

*See `docs/dev_config.md` for more detailed field documentation.*

## 3. Pipeline implementation

Create your characterisation pipeline module in `src/cherami/pipelines/` (e.g., `src/cherami/pipelines/my_pipeline.py`).

Your module must export:
1. A `Pipeline` subclass.
2. A `build_pipeline` factory function.
3. A `build_worker` factory function.

### Example Implementation

```python
from pathlib import Path
import csv

from cherami.config import PipelineConfig, WorkerConfig
from cherami.pipelines.pipeline import Pipeline
from cherami.pipelines.worker import Worker


class MyPipeline(Pipeline):
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        ## For example here make a basic samplesheet
        ## An actual implementation would probably query onyx for relevant fields/data
        ## As a matter of convention try and avoid importing pandas for this (as to not add a dependency)
        rows = []
        for sample_id in samples:
            rows.append({
                "sample_id": sample_id,
                "fastq_1": f"/data/{sample_id}_R1.fastq.gz",
                "fastq_2": f"/data/{sample_id}_R2.fastq.gz"
            })

        with output_filepath.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=["sample_id", "fastq_1", "fastq_2"])
            writer.writeheader()
            writer.writerows(rows)


def build_pipeline(pipeline_config: PipelineConfig) -> Pipeline:
    return MyPipeline(pipeline_config)


def build_worker(
    worker_config: WorkerConfig,
    pipeline_config: PipelineConfig,
    work_dir: Path,
    output_dir: Path,
) -> Worker:
    ## this uses the default worker implementation - which is fine if you dont need any custom behaviour (see below section)
    pipeline = build_pipeline(pipeline_config)
    return Worker(worker_config, pipeline, work_dir, output_dir)
```

**Notes:**
- `samples`: Currently a single-item list, but designed to support batching in future.
- `job_id`: This is derived from the message payload (`match_uuid`); used for Kubernetes job naming.
- **Samplesheet**: The format must exactly match what your Nextflow pipeline expects.

## 4. Pipeline Logic & Customisation

The `Pipeline` class implements a function to define the decision logic of the characterisation pipeline.

### 4.1. Decision logic (`should_run`)
Override `should_run(sample_id)` to filter samples before they run. This allows you to say things like "only run the strep pipeline if a sample has > 10 strep reads" for example.

```python
def should_run(self, sample_id: str) -> bool:
    ## return false to skip a pipeline, true to run it.
    return check_read_count(sample_id) > 10
```

If `False`, the worker calls `on_skip()` and acknowledges the message immediately removing it from the message queue.

### 4.2. Nextflow process validation (`proc_names`)
You can define valid exit codes for specific Nextflow processes. By default, all processes must exit with `0`.

```python
@property
def proc_names(self) -> dict[str, list[int]]:
    return {
        "fastqc": [0],
        "other_process": [0, 1]  ## this will allow an exit code of 1 (fail) for this process if needed for any reason
    }
```

### 4.3. Other checks before pipeline executes (`validate`)
Override `validate()` to perform setup checks (e.g., verifying external files exist) before execution starts.

## 5. Worker implementation

### Standard Worker
The default `Worker` class (`src/cherami/pipelines/worker.py`) is sufficient for most use cases. It:
1. Listens to the configured exchange.
2. Launches the pipeline.
3. On success: Optionally republishes to a downstream queue (if `publish_queue_suffix` is set).
4. On failure: Retries up to `max_retries` before giving up.

### Custom Worker
You can however, extend the base `Worker` if you need custom orchestration logic:
- **Custom Payloads**: Processing messages that don't fit `climb_id`/`match_uuid`.
- **Additional exchanges**: Publishing to a success or fail queue.
- **Error handling**: Dead-letter queues, alerting systems, or custom retry backoffs.

To do this, subclass `Worker` and override the relevant hooks:
- `on_skip(message)`
- `on_success(message, payload)`
- `on_retry(message)`
- `on_sample_failure(message)`

Return your new subclass from `build_worker` in your pipeline module.

## 6. Running and Verifying

Use the CLI to verify your configuration and logic.

**Start the worker:**
```bash
uv run cherami serve path/to/config.json
```

**Inspect queues:**
```bash
uv run cherami describe path/to/config.json
```

**Dry-run decision logic:**
Evaluates `should_run()` for specific samples without spawning full jobs.
```bash
uv run cherami evaluate path/to/config.json SAMPLE_1 SAMPLE_2
```
