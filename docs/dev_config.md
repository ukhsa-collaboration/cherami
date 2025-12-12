# Config Specification

Cherami loads all of the required runtime settings from a config file.

## 1. Using the config file

- Pass `--config /path/to/config.json` to any `cherami` CLI command or set the `CHERAMI_CONFIG` environment
  variable.
- Environment specific defaults can be found under `./configs/` (for example `configs/cherami_synthscape.json`). Copy and edit one
  of these when creating a new environment-specific configuration.

## 2. Config structure

The config should have 3 sections: a global block, and sections to configure workers and pipelines

```json
{
  "global": {
    "work_dir": "",
    "output_dir": ""
  },
  "pipelines": {
    "my-pipeline": { }
  },
  "workers": {
    "my-worker": { }
  }
}
```
IMPORTANT:
- The `global` section defines shared `work_dir`and `output_dir` roots used by every pipeline 
- Cherami will create `<work_dir>/<worker_name>/<climb_id>/` and `<output_dir>/<worker_name>/<climb_id>/` automatically for samples,
- Pipeline keys (e.g. `"my-pipeline"`) must match the names registered in `src/cherami/pipelines/__init__.py`.
- Worker keys (e.g. `"my-worker"`) must match the names in `src/cherami/workers/__init__.py`.
- Names should NOT contain underscores (`_`) as these are NOT valid kuberentes job names.

## 3. Global fields

| Field | Required | Description |
| --- | --- | --- |
| `work_dir` | Yes | Directory used for all intermediate files. |
| `output_dir` | Yes | Base directory for published outputs and trace files. |

## 4. Pipeline fields

| Field | Required | Description |
| --- | --- | --- |
| `version` | Yes | The pipeline version. |
| `path` | Yes | Path to the nextflow pipeline given to `nextflow run`. Should typically be a github repository containing the pipeline. |
| `cpus`, `mem`, `cpu_limit`, `mem_limit` | Yes | Kubernetes resource limits. |
| `nf_config_path` | Optional | Path to any extra Nextflow config file (e.g containing k8 executor profiles). |
| `nf_profiles` | Optional | List of profiles passed to the nextflow command (e.g `-profile profile1,profile2`). |
| `nf_extra_args` | Optional | Any additional arguments appended to the Nextflow command. |
| `namespace` | Yes | Kubernetes namespace for the `Job`. |
| `container` | Yes | Container used to run Nextflow. |
| `backoff_limit` | Yes | Maximum failed pod restarts before the job is considered failed. |
| `max_retries` | Yes | Number of times Cherami will retry a failed run for a given sample. |
| `retry_timeout` | Yes | How long to wait in seconds between job retries. |
| `job_timeout` | Yes | Maximum run time in seconds before Cherami treats the job as timed out. |


## 5. Worker fields

| Field | Required | Description |
| --- | --- | --- |
| `pipeline_name` | Yes | Name of the pipeline the worker runs; must match an entry under `"pipelines"` in the same config file. |
| `listen_exchange` | Yes | RabbitMQ exchange the worker consumes from. |
| `listen_queue_suffix` | Yes | Queue suffix for this worker - varys combines this with the exchange name to form the queue name. |
| `publish_queue_suffix` | Optional | When set, successful runs publish the original message to this queue. The exchange defaults to `listen_exchange` unless `publish_exchange` is also provided. |
| `publish_exchange` | Optional | Exchange used for completion messages when `publish_queue_suffix` is set. |
| `varys_config_path` | Yes | Path to the Varys configuration file. |
| `varys_log_path` | Yes | Path to the Varys log file for this worker. |
