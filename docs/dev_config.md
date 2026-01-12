# Config Specification

Cherami loads all of the required runtime settings from a JSON config file.

## 1. Config structure

The config has 3 sections: a global block, a single pipeline block, and a single worker block.

```json
{
  "global": {
    "work_dir": "",
    "output_dir": ""
  },
  "pipeline": { },
  "worker": { }
}
```
IMPORTANT:
- The `global` section defines shared `work_dir` and `output_dir` roots used by the pipeline.
- Cherami will create `<work_dir>/<pipeline_name>/` and `<output_dir>/<pipeline_name>/` automatically.
- The pipeline `name` must match a module in `src/cherami/pipelines/`:
  - `name` uses hyphens (e.g. `orange-box`)
  - the module filename uses underscores (e.g. `orange_box.py`)
- Names should NOT contain underscores (`_`) as these are NOT valid kuberentes job names.

## 2. Global fields

| Field | Required | Description |
| --- | --- | --- |
| `work_dir` | Yes | Directory used for all intermediate files. |
| `output_dir` | Yes | Base directory for published outputs and trace files. |

## 3. Pipeline fields

| Field | Required | Description |
| --- | --- | --- |
| `name` | Yes | Pipeline name (used for pipeline module loading and Kubernetes job name prefix). |
| `version` | Yes | The pipeline version. |
| `path` | Yes | Path to the nextflow pipeline given to `nextflow run`. Should typically be a github repository containing the pipeline. |
| `cpus`, `mem`, `cpu_limit`, `mem_limit` | Yes | Kubernetes resource limits. |
| `nf_config_path` | Yes | Path to any extra Nextflow config file (e.g containing k8 executor profiles). |
| `nf_profiles` | Yes | List of profiles passed to the Nextflow command (e.g `-profile profile1,profile2`). |
| `nf_extra_args` | Yes | Any additional arguments appended to the Nextflow command. |
| `namespace` | Yes | Kubernetes namespace for the `Job`. |
| `container` | Yes | Container used to run Nextflow. |
| `backoff_limit` | Yes | Maximum failed pod restarts before the job is considered failed. |
| `max_retries` | Yes | Number of times Cherami will retry a failed run for a given sample. |
| `retry_timeout` | Yes | How long to wait in seconds between job retries. |
| `job_timeout` | Yes | Maximum run time in seconds before Cherami treats the job as timed out. |


## 4. Worker fields

| Field | Required | Description |
| --- | --- | --- |
| `listen_exchange` | Yes | RabbitMQ exchange the worker consumes from. |
| `listen_queue_suffix` | Yes | Queue suffix for this worker - varys combines this with the exchange name to form the queue name. |
| `publish_queue_suffix` | Optional | When set, successful runs publish the original message to this queue. The exchange defaults to `listen_exchange` unless `publish_exchange` is also provided. |
| `publish_exchange` | Optional | Exchange used for completion messages when `publish_queue_suffix` is set. |
| `varys_config_path` | Yes | Path to the Varys configuration file. |
| `varys_log_path` | Yes | Path to the Varys log file for this worker. |

## 5. Minimal example

This is the minimal shape of a config file. Use `configs/cherami_amr.json` and
`configs/cherami_orange_box.json` for complete examples.

```json
{
  "global": {
    "work_dir": "/path/to/work",
    "output_dir": "/path/to/output"
  },
  "pipeline": {
    "name": "my-pipeline",
    "version": "0.1.0",
    "path": "/path/to/nextflow/project",
    "cpus": 4,
    "mem": "8G",
    "cpu_limit": 4,
    "mem_limit": "8G",
    "nf_config_path": "/path/to/nextflow.config",
    "nf_profiles": ["docker"],
    "nf_extra_args": [],
    "namespace": "my-namespace",
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
    "varys_config_path": "./my/varys/config.cfg",
    "varys_log_path": "./my_pipeline_varys.log"
  }
}
```
