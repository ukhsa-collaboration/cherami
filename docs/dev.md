## Development

Below contains (informal) development notes and guidance for contributors.

### Installation for development
The reccomended way of installing this repo is using uv:

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

### Code style

This repo uses ruff for formatting and linting, enforced via CI and a pre-commit hook both of which are included in the dev dependencies. Ensure these are installed (including the pre-commit) so that CI doesnt fail for format/linting errors.

In general it is enough to follow the ruff lint rules (defined in the `pyproject.toml`) and ensure there are no violations using `ruff check` and `ruff format`.

Every function should have its return signature and arguments type hinted (this is enforced via ruff).

Tests will be run in CI, but should be run locally first with `uv run pytest`

Docstrings should be in [google](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) style in full for any public or exported functions/classes. Private functions can have smaller single line docstrings without explictly documenting `Arguments` and `Returns`. 

## Setup for local dev

### Setting up a local RabbitMQ server for development

A RabbitMQ pod can be created using `deploy_rabbitmq.sh` helper in `./scripts`. This creates a kubernetes pod running a RabbitMQ server and prints its IP address.

To create a new exchange you can use the CLI tool `rabbitmqadmin` from the container:

```bash
kubectl exec -it rabbitmq -- /bin/bash 
rabbitmqadmin -u admin -p password declare exchange name=cherami_test type=fanout durable=true
```

You will need to update the varys config file to point to the IP of the local pod.

An example varys config file for this configuration:
```json
{
  "version": "0.1",
  "profiles": {
    "cherami": {
      "username": "admin",
      "password": "password",
      "amqp_url": "10.0.0.1",
      "port": 5672,
      "use_tls": false
    }
  }
}
```

Then run `cherami run <worker>` to listen to messages sent on the created exchange

An example helper script to test payloads is included in `./scripts/send.py` e.g:
```bash
uv run scripts/send.py
```

### Workers and Pipelines

Cherami has two broad concepts, workers and pipelines. Workers exist as multiprocessing processess and consume messages from RabbitMQ queues to coordinate pipeline execution. Pipelines are templates that describe how to construct and run Kubernetes jobs for Nextflow workflows.

#### Workers

Workers implement the message queue driven managment. Each worker binds to a specific RabbitMQ exchange and queue, watches for incoming sample messages, and hands them off to a pipeline runner. Workers live in `src/cherami/workers/` and MUST inherit from the `Worker` base class.

The base worker provides a full set of methods to handle an incoming sample. When a worker spawns, it initialises connections to RabbitMQ (via Varys) and Kubernetes, and will continually listen on its configured message queue. For each message received, the worker reads the payload, checks whether the configured pipeline should run for that sample, and if so, uses the `PipelineRunner` to actually execute the job in k8s. The worker tracks retry attempts per sample and handles message acknowledgment based on whether the pipeline succeeded, failed, or should be retried.

To function worker subclasses only need to implement `get_pipeline()` to specify which pipeline template the worker should use. Everything else is handled by the base class, though you can override hook methods if you need custom behavior at specific events.

##### Adding a new Worker

###### 1. Implement the worker

Create a new module under `src/cherami/workers/` that subclasses `Worker`. In `__init__`, pass the worker metadata to the base class. In `get_pipeline()`, return an instance of the pipeline you want to run.

```python
from pathlib import Path

from cherami.pipelines.implementations.amr import AmrPipeline
from cherami.workers.base import Worker

class ExampleWorker(Worker):
    def __init__(self) -> None:
        super().__init__(
            worker_name="example",
            listen_exchange="cherami_test",
            listen_queue_suffix="example_queue",
            varys_config_path=Path("./conf/varys.cfg"),
            varys_log_path=Path("./example_varys.log"),
            ## optional: publish to another queue when successful
            ## publish_queue_suffix="downstream_queue",
            ## publish_exchange="another_exchange",
        )

    def get_pipeline(self) -> AmrPipeline:
        return AmrPipeline()
```

The `varys_config_path` should point to a JSON file containing RabbitMQ credentials and connection details. The `varys_log_path` is where Varys will write its own logs (separate from the worker logs).

If you set `publish_queue_suffix`, the worker will push successful messages to that queue on the same exchange by default (or to `publish_exchange` if you set that).

###### 2. Register the worker

Add your worker class to the `WORKERS` dictionary in `src/cherami/workers/__init__.py`:

```python
from cherami.workers import example
from cherami.workers.base import Worker

WORKERS: dict[str, type[Worker]] = {
    "example": example.ExampleWorker,
    ## existing workers...
}
```

The key you choose here is the name you will use on the command line to start the worker. 

###### 4. Add tests

Write tests for any custom logic in your worker (overridden hooks, custom validation).

#### Pipelines

Pipelines are configuration templates that describe how to run a Nextflow workflow as a Kubernetes job. They specify resource requests and limits, the Nextflow command to run, where to write output, and how to validate success. When a worker calls `PipelineRunner.run_pipeline()`, the runner uses the pipeline template to construct a Kubernetes job manifest and submit it.

Pipelines live in `src/cherami/pipelines/implementations/` and inherit from the `Pipeline` base class. The base class handles job manifest generation, trace file evaluation, and validation. Subclasses provide a `PipelineConfig` and implement `generate_samplesheet()` to prepare inputs for their specific Nextflow workflow.

##### Pipeline configuration and job manifests

Each pipeline defines a `config` property that returns a `PipelineConfig`. This dataclass contains everything needed to construct the Kubernetes job:

The `Pipeline.create_job_manifest()` method uses this config to build a Kubernetes Job manifest. The manifest includes volume mounts, environment variables (e.g. onyx credentials), and the Nextflow command assembled from the config.

##### Samplesheet generation

The abstract method `generate_samplesheet(samples: list[str], job_id: str)` is where you define how to prepare inputs for your pipeline. This method receives a list of sample IDs (usually just one) and a unique job ID, and should return a path to a CSV samplesheet file (or `None` if the pipeline does not require a samplesheet).

Implementations typically query Onyx to fetch any s3 file paths and metadata for each sample, then write a CSV with the columns the Nextflow workflow expects. The samplesheet path is passed to Nextflow via `--samplesheet` in the job command.

##### Success evaluation

When a Kubernetes job completes successfully (pod exits 0), the pipeline runner calls `pipeline.evaluate_exit_status(trace_file)` to check whether the Nextflow processes actually succeeded. Nextflow writes a trace file (`pipeline_trace.txt`) that records the exit code of every process. The base `Pipeline.evaluate_exit_status()` method reads this file and checks exit codes.

By default, every process must exit with code 0. If your pipeline has processes that are expected to fail under certain conditions, you can override the `proc_names` property to return a dictionary mapping process names to lists of allowed exit codes. For example, if a process called `CLASSIFY` is allowed to exit with 0 or 1, you would return `{"CLASSIFY": [0, 1]}`. The evaluation will only check the processes you list; any processes not in the dictionary are ignored.

##### Decision logic

The `should_run(sample_id: str)` method lets you gate execution based on sample metadata or other conditions. The default implementation returns `True` (always run). If you override it to return `False` for certain samples, the worker will skip those samples and call `on_skip()` instead of launching the pipeline. This is useful if you only want to run the pipeline for samples that meet certain criteria (e.g. run strep typing pipeline, only if > 5000 reads of strep).

##### Adding a new Pipeline

###### 1. Defining the pipeline template

Create a new module in `src/cherami/pipelines/implementations/` and subclass `Pipeline`. Define a `config` property that returns a `PipelineConfig` with all the required settings. Implement `generate_samplesheet()` to prepare inputs specific to your workflow.

```python
from cherami.pipelines.base import Pipeline, PipelineConfig

class MpoxPipeline(Pipeline):
    pipeline_name = "mpox-pipeline"

    @property
    def config(self) -> PipelineConfig:
        return PipelineConfig(
            name=self.pipeline_name,
            version="0.1.0",
            path="/shared/team/projects/nf-mpox",
            cpus=4,
            mem="8G",
            cpu_limit=4,
            mem_limit="8G",
            nf_config_path=Path("/shared/team/projects/nf-mpox/nextflow.config"),
            nf_profiles=["docker"],
            nf_extra_args=[],
            work_dir=Path("/shared/team/projects/mpox/work"),
            output_dir=Path("/shared/team/projects/mpox/output"),
            namespace="ns-example",
            container="quay.io/climb-tre/nextflow",
            backoff_limit=5,
            max_retries=1,
            retry_timeout=10,
            job_timeout=3600,
        )

    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        ## query onyx for sample metadata, construct CSV
        rows = []
        for sample_id in samples:
            row = {"sample": sample_id, "fastq_1": "...", "fastq_2": "..."}
            rows.append(row)

        samplesheet_path = self.config.work_dir / "samplesheets" / f"{job_id}.csv"
        samplesheet_path.parent.mkdir(parents=True, exist_ok=True)

        ## write the csv
        import csv
        with samplesheet_path.open("w") as f:
            ...

        return samplesheet_path
```

If your pipeline has processes that can exit with non-zero codes, override the `proc_names` property:

```python
@property
def proc_names(self) -> dict[str, list[int]]:
    return {
        "CLASSIFY": [0, 1],  ## allowed to fail
        "ASSEMBLE": [0],      ## must succeed
    }
```

If you need to gate execution based on sample properties, override `should_run()`:

```python
def should_run(self, sample_id: str) -> bool:
    ## check if sample has required metadata
    ## return False to skip this sample
    return True
```

###### 2. Registering the pipeline

Pipelines do not need explicit registration in a dictionary like workers do. They are used directly by workers. However, ensure the module is imported and available by adding an import to `src/cherami/pipelines/implementations/__init__.py`:

```python
from cherami.pipelines.implementations.mpox import MpoxPipeline
```

###### 3. Add tests for the new pipeline

Write tests for `generate_samplesheet()` to ensure it produces the correct CSV format for your Nextflow workflow.
