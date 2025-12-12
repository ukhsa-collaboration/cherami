# Adding a new pipeline

This document describes how to add a new pipeline to cherami.

## 1. Overview

- A `Pipeline` is a class that wraps all of the configuration required to run a Nextflow pipeline via Kubernetes.
- Pipeline behaviour is defined by:
  - A config entry under `"pipelines"`.
  - A Python subclass of `Pipeline`.
- The `Pipeline` class defines how to:
  - Generate a samplesheet for a given sample.
  - Create a Kubernetes `Job` manifest for the Nextflow run.
  - Evaluate the Nextflow trace file.
- Therefore each nextflow pipeline that will be run via cherami should have a `Pipeline` class implemented for it.
- Cherami reads pipeline settings from the provided config file to construct a `PipelineConfig` object which becomes a component of the `Pipeline` class.
- A `Pipeline` is standalone to a `Worker`. A `Worker` has a `Pipeline` associated with it - but are seperate ideas. Broadly a `Pipeline` handles its own configuration, when to run, how to evaluate it was successful etc. Whereas a `Worker` handles the message queue orchestration and actual execution in that context.
  - As these are seperate concepts it is possible to run a given pipeline without a worker using the `cherami run` subcommand - which can be useful for debugging.

## 2. Add the pipeline to the config file

Any config with the expected structure can be used via the `--config` CLI option or `CHERAMI_CONFIG` environment variable, allowing for different configurations for different environments (i.e synthscape vs mSCAPE). The repo contains configration (`./configs`) for common runtime environments already - therefore it is likely a new pipeline should be added to one of these.

Pipelines are configured under the top-level `"pipelines"` key in the config.

1. Open the appropriate config file for the environment you will be using (for example `configs/cherami_synthscape.json`).
2. Under `"pipelines"`, add a new object for the name of the pipeline you are adding (for example `"my-pipeline"`).
3. Populate all required fields:

IMPORTANT:
- Pipeline names should NOT contain underscores (`_`) as these are NOT valid kuberentes job names.

```json
{
  "pipelines": {
    "my-pipeline": {
      "version": "0.1.0",
      "path": "/path/to/nextflow/project",
      "cpus": 4,
      "mem": "8G",
      "cpu_limit": 4,
      "mem_limit": "8G",
      "nf_config_path": "/path/to/nextflow.config",
      "nf_profiles": ["docker"],
      "nf_extra_args": [],
      "namespace": "k8s-namespace",
      "container": "quay.io/path/to/image",
      "backoff_limit": 5,
      "max_retries": 1,
      "retry_timeout": 10,
      "job_timeout": 3600
    }
  }
}
```

Expected fields are as follows:

| Field | Description |
| --- | --- |
| `version` | The pipeline version. |
| `path` | Path to the nextflow pipeline given to `nextflow run`. Should typically be a github repository containing the pipeline. |
| `cpus`, `mem`, `cpu_limit`, `mem_limit` | Kubernetes resource limits. |
| `nf_config_path` | Path to any extra Nextflow config file (e.g containing k8 executor profiles). |
| `nf_profiles` | List of profiles passed to the nextflow command (e.g `-profile profile1,profile2`). |
| `nf_extra_args` | Any additional arguments appended to the Nextflow command. |
| `namespace` | Kubernetes namespace for the `Job`. |
| `container` | Container used to run Nextflow. |
| `backoff_limit` | Maximum failed pod restarts before the job is considered failed. |
| `max_retries` | Number of times Cherami will retry a failed run for a given sample. |
| `retry_timeout` | How long to wait in seconds between job retries. |
| `job_timeout` | Maximum run time in seconds before Cherami treats the job as timed out. |


## 3. Implement the Pipeline subclass

Pipelines are implemented in `src/cherami/pipelines`. Each pipeline is a subclass of `Pipeline`.

1. Create a new file `src/cherami/pipelines/my_pipeline.py`.
2. Implement a `Pipeline` subclass that at minimum defines `generate_samplesheet`.
3. Optionally override `should_run`, `proc_names`, and `validate`.

### 3.1. Basic structure

```python
from pathlib import Path

from cherami.pipelines.pipeline import Pipeline


class MyPipeline(Pipeline):
    def generate_samplesheet(self, samples: list[str], job_id: str, output_filepath: Path) -> None:
        ## Look up any metadata you need in onyx
        ## Write a samplesheet file to the provided filepath
        ...
```

IMPORTANT:

- `samples` currently should only ever be a list of one sample ID (`[sample_id]`), but accepts a list for future batching.
- `job_id` is the UUID for this run obtained via the message payload and is used to name the Kubernetes job and generated samplesheets (work and output directories always use `climb_id`).
- The samplesheet format is pipeline-specific. It must match what your Nextflow pipeline expects.
- If your pipeline needs metadata from Onyx, follow the example used by `AmrPipeline` in `src/cherami/pipelines/amr.py`.
  - However, cherami does not require Onyx. Any method of constructing a valid samplesheet is acceptable as long as `generate_samplesheet` returns the correct path.

### 3.3. Optional: should_run

The default implementation of `Pipeline.should_run(sample_id)` always returns `True`. This means that by default, the pipeline will run for every sample.

Override this method to add decision logic, for example to check Onyx fields pass thresholds before launching a job

```python
    def should_run(self, sample_id: str) -> bool:
        ## Return False to skip running the pipeline for this sample_id
        return True
```

IMPORTANT:
- When `should_run` returns `False`:
  - cherami will skip the pipeline for that sample.
  - Workers will call their `on_skip` handler instead of launching a job.

### 3.4. Optional: proc_names

The default implementation of `Pipeline.evaluate_exit_status` requires that every process in the Nextflow trace file exits with code `0`.

Override the `proc_names` property to:

- Allow specific non-zero exit codes for particular processes.
- Limit evaluation to a subset of processes.

```python
    @property
    def proc_names(self) -> dict[str, list[int]]:
        return {
          ## allow a process to exit 5 without failing the validation check
            "PROCESS_NAME_IN_TRACE": [0, 5],
        }
```

The keys in this mapping must match the `name` column in the Nextflow trace file.

### 3.5. Optional: validate

The default implementation of `validate` method only checks that configured filesystem locations exist.

Override `validate` if your pipeline needs additional checks before runs start, for example:

- Confirm that any required environment variables are set.
- Check that external configuration files or database connections exist.

```python
    def validate(self) -> None:
        super().validate()
        ## add any pipeline-specific validation here
```

## 4. Register the pipeline

Cherami looks up pipelines by name using the `PIPELINES` mapping defined in `src/cherami/pipelines/__init__.py`.

1. Open `src/cherami/pipelines/__init__.py`.
2. Import your new pipeline class.
3. Add it to the `PIPELINES` dictionary with the same key used in your config.

Example:

```python
from cherami.pipelines.amr import AmrPipeline
from cherami.pipelines.orange_box import OrangeBoxPipeline
from cherami.pipelines.my_pipeline import MyPipeline
from cherami.pipelines.pipeline import Pipeline  # noqa: F401

PIPELINES: dict[str, type[Pipeline]] = {
    "amr": AmrPipeline,
    "orange-box": OrangeBoxPipeline,
    "my-pipeline": MyPipeline,
}
```
IMPORTANT:
- `"my-pipeline"` must match the pipeline name in the config file (for example the key under `"pipelines"`).
- Pipeline names should NOT contain underscores (`_`) as these are NOT valid kuberentes job names.

## 6. Running and testing the pipeline

Once the config and Python class are in place and registered:

1. Ensure the config file contains the new pipeline entry with the correct name.
2. Ensure that the pipeline name does not contain an underscore (`_`)
3. Write tests for any overridden behaviour.

There are some useful debug commands that can help testing new pipelines.

To check if a sample will pass any custom decision logic defined in the `should_run` method, you can use `cherami evaluate`:

```bash
CHERAMI_CONFIG=path/to/config.json 
uv run cherami evaluate SAMPLE_ID --pipelines my-pipeline
```
You can also run the pipeline directly for a sample without the need for a worker:

```bash
CHERAMI_CONFIG=path/to/config.json 
uv run cherami run SAMPLE_ID --pipelines my-pipeline
```
