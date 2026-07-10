# Configuration Specification

Cherami loads all required runtime settings from a JSON configuration file.

## 1. Structure

The configuration file consists of three mandatory sections:
1. `global`: Shared settings.
2. `pipeline`: Configuration for the characterisation pipeline logic.
3. `worker`: Configuration for the `Worker` orchestration.

```json
{
  "global": {
    "work_dir": "",
    "output_dir": "",
    "server": ""
  },
  "pipeline": { },
  "worker": { }
}
```

### Important Notes
- The `global` section defines shared `work_dir` and `output_dir` roots.
- Cherami automatically creates subdirectories: `<work_dir>/<pipeline_name>/` and `<output_dir>/<pipeline_name>/` and so globals in most cases should be set the same for ALL pipelines.
- The pipeline `name` determines which Python module is loaded from `src/cherami/pipelines/`:
  - `name` must be hyphenated (e.g., `orange-box`).
  - The corresponding module file must be underscored (e.g., `orange_box.py`).
- Pipeline names must NOT contain underscores (`_`) as they are used to generate Kubernetes Job names, which do not support underscores.

## 2. Global Fields

| Field | Required | Description |
| --- | --- | --- |
| `work_dir` | Yes | Root directory for all intermediate files and working directories. |
| `output_dir` | Yes | Root directory for final published outputs and trace files. |
| `server` | Yes | The name of the server where Onyx will be queried. |

## 3. Pipeline Fields

These settings configure the `Pipeline` object and the Kubernetes Jobs it spawns.

| Field | Required | Description |
| --- | --- | --- |
| `name` | Yes | The identifier for the characterisation pipeline. Used for module loading and Kubernetes Job naming. |
| `version` | Yes | The version string of the pipeline logic. Should specify Git branch, tag, or commit for Nextflow to pull. |
| `path` | Yes | File system path to the Nextflow pipeline project (typically a GitHub repository). |
| `cpus` | Yes | Kubernetes CPU request for the job. |
| `mem` | Yes | Kubernetes memory request (e.g., "8G"). |
| `cpu_limit` | Yes | Kubernetes CPU limit. |
| `mem_limit` | Yes | Kubernetes memory limit. |
| `nf_config_path` | Yes | Path to an additional Nextflow config file (e.g., for Kubernetes executor profiles). |
| `nf_profiles` | Yes | List of Nextflow profiles to apply (e.g., `["docker", "test"]`). |
| `nf_extra_args` | Yes | List of additional arguments to append to the Nextflow command. |
| `namespace` | Yes | Kubernetes namespace where the Job will run. |
| `container` | Yes | Container image used to execute the Nextflow head process. |
| `backoff_limit` | Yes | Maximum number of pod restarts allowed before the Job is marked as failed. |
| `max_attempts` | Yes | Total number of attempts Cherami will make for a failed sample analysis (must be at least 1). |
| `retry_timeout` | Yes | Wait time (in seconds) between retries. |
| `job_timeout` | Yes | Maximum execution time (in seconds) before the Job is timed out. |

## 4. Worker Fields

These settings configure the `Worker` instance that orchestrates the pipeline.

| Field | Required | Description |
| --- | --- | --- |
| `listen_exchange` | Yes | The RabbitMQ exchange the worker listens to. |
| `listen_queue_suffix` | Yes | Suffix for the queue name. Varys combines this with the exchange name. |
| `publish_queue_suffix` | Optional | If set, successful runs publish the original message to this queue. |
| `publish_exchange` | Optional | The exchange used for publishing success messages. Defaults to `listen_exchange` if not set. |
| `varys_config_path` | Yes | Path to the Varys configuration file (`varys.cfg`). |
| `varys_log_path` | Yes | Path where the worker's Varys log should be written. |

## 5. Minimal Example

Below is a minimal valid configuration. See `configs/cherami_amr.json` or `configs/cherami_orange_box.json` for production examples.

```json
{
  "global": {
    "work_dir": "/path/to/work",
    "output_dir": "/path/to/output",
    "server": "server"
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
    "namespace": "my-server-namespace",
    "container": "quay.io/climb-tre/nextflow",
    "backoff_limit": 5,
    "max_attempts": 2,
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
