# Config

Each pipeline in Cherami has its own JSON config file. You pass this file to Cherami when you start a worker with `cherami serve <config.json>`. This step shows how to write a test config for your pipeline.

tldr:

- The config has three sections: [`global`](#global), [`pipeline`](#pipeline) and [`worker`](#worker).
- Every key must be present. If a key has no value, set it to `null`.
- [Start from the template](#start-from-the-template) below and change the values for your pipeline.
- Set `work_dir` and `output_dir` to your own [test directories](#global).
- After you change the config, [restart the worker](#changing-the-config).

## Start from the template

Copy this template and change the values in `< >`. The other values are the same for every pipeline in the test environment.

```json
{
  "global": {
    "work_dir": "<path/to/test/work_dir>",
    "output_dir": "<path/to/test/output_dir>",
    "server": "devscape"
  },
  "pipeline": {
    "name": "<my-pipeline>",
    "version": "<branch or tag to run>",
    "path": "<organisation/repo>",
    "cpus": 4,
    "mem": "8G",
    "cpu_limit": 4,
    "mem_limit": "8G",
    "nf_config_path": "/shared/team/configs/nextflow/nextflow.config",
    "nf_profiles": [],
    "nf_extra_args": [
      "-e.ONYX_DOMAIN=$ONYX_DOMAIN",
      "-e.ONYX_TOKEN=$ONYX_TOKEN",
      "-latest"
    ],
    "namespace": "ns-ukhsa-devscape",
    "container": "quay.io/climb-tre/nextflow:25.04.8",
    "backoff_limit": 0,
    "max_attempts": 2,
    "retry_timeout": 10,
    "job_timeout": 3600
  },
  "worker": {
    "listen_exchange": "downstream-ukhsa_orange_box-devscape",
    "listen_queue_suffix": "<ukhsa_mypipeline_worker>",
    "publish_queue_suffix": null,
    "publish_exchange": null,
    "rerun_queue_suffix": null,
    "rerun_exchange": null,
    "priority_queue_suffix": null,
    "priority_exchange": null,
    "varys_config_path": "/shared/team/configs/varys/varys.conf",
    "varys_log_path": "/shared/team/logs/varys/<my_pipeline>_varys.log"
  }
}
```

If a key is missing, Cherami will raise an error and the worker will not start. For complete configs from existing pipelines, see the [configs repo](TODO: link).

# Config sections in detail

## `global`

The `global` section contains the root directories for work files and outputs, and the name of the Onyx server. For testing, set `work_dir` and `output_dir` to your own directories under `/shared/team/`.

In each root directory, Cherami will create a subdirectory for each pipeline, sample and run: `<pipeline_name>/<sample_id>/<timestamp>/`.

| Field        | Nullable | Description                                              |
| ------------ | -------- | -------------------------------------------------------- |
| `work_dir`   | No       | The root directory for Nextflow work files.              |
| `output_dir` | No       | The root directory for outputs.                          |
| `server`     | No       | The name of the Onyx server. Use `devscape` for testing. |

## `pipeline`

The `pipeline` section contains settings configuring the pipeline runs.

### Name, version and repo

| Field     | Nullable | Description                                                                                                                                                                                                                                                                          |
| --------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `name`    | No       | The name of your pipeline. Use hyphens, not underscores, e.g. `strep-pneumo`. Kubernetes job names contain the pipeline name and cannot contain underscores. The pipeline module has the same name with underscores, e.g. `strep_pneumo.py`. You will create this module in step 03. |
| `version` | No       | The branch or tag Nextflow will run. Your pipeline must also write this exact string as `pipeline_version` in the analysis records. For testing, use your development branch.                                                                                                        |
| `path`    | No       | The GitHub repo for your pipeline, in the form `organisation/repo`.                                                                                                                                                                                                                  |

### Nextflow options

| Field            | Nullable | Description                                                                                                                                                                                                                                                                                       |
| ---------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `nf_config_path` | No       | The shared team Nextflow config. Do not change this value.                                                                                                                                                                                                                                        |
| `nf_profiles`    | No       | A list of Nextflow profiles to use, e.g. `["docker"]`. If your pipeline does not use profiles, set an empty list (`[]`).                                                                                                                                                                          |
| `nf_extra_args`  | No       | A list of extra arguments for the `nextflow run` command. The template contains `-e.ONYX_DOMAIN` and `-e.ONYX_TOKEN`, which give the Onyx credentials to Nextflow. With `-latest`, Nextflow pulls the latest commit of your branch before each run. Add other arguments that your pipeline needs. |

### Resources and retries

Cherami will create one Kubernetes job for each run. These fields set the resources, limits and timeouts for the job.

| Field           | Nullable | Description                                                                                      |
| --------------- | -------- | ------------------------------------------------------------------------------------------------ |
| `cpus`          | No       | Kubernetes CPU request for the job.                                                              |
| `mem`           | No       | Kubernetes memory request, e.g. `"8G"`.                                                          |
| `cpu_limit`     | No       | Kubernetes CPU limit.                                                                            |
| `mem_limit`     | No       | Kubernetes memory limit.                                                                         |
| `namespace`     | No       | Kubernetes namespace where the job will run.                                                     |
| `container`     | No       | The container image that runs Nextflow.                                                          |
| `backoff_limit` | No       | The maximum number of pod failures before the job fails.                                         |
| `max_attempts`  | No       | The maximum number of attempts for each sample. Must be at least 1.                              |
| `job_timeout`   | No       | The maximum run time of a job, in seconds. After this time, Cherami will mark the job as failed. |
| `retry_timeout` | No       | Not currently used.                                                                              |

## `worker`

These fields set the RabbitMQ exchanges and queues for the worker. Step 06 gives more information about exchanges and queues.

| Field                   | Nullable | Description                                                                                                                                       |
| ----------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `listen_exchange`       | No       | The exchange that the worker consumes messages from. For pathchar pipelines, use the orange box exchange, `downstream-ukhsa_orange_box-<server>`. |
| `listen_queue_suffix`   | No       | The name of the worker queue. Each pipeline must have a different queue. Use the format `ukhsa_<pipeline>_worker`.                                |
| `publish_exchange`      | Yes      | The exchange to publish to when a run succeeds. Set this only if another pipeline must run after your pipeline. If not, set it to `null`.         |
| `publish_queue_suffix`  | Yes      | The queue suffix to publish to when a run succeeds. Set this only if another pipeline must run after your pipeline. If not, set it to `null`.     |
| `rerun_exchange`        | Yes      | The exchange for rerun messages. Only orange box uses this field. Set it to `null`.                                                               |
| `rerun_queue_suffix`    | Yes      | The queue suffix for rerun messages. Only orange box uses this field. Set it to `null`.                                                           |
| `priority_exchange`     | Yes      | The exchange for priority messages. Only orange box uses this field. Set it to `null`.                                                            |
| `priority_queue_suffix` | Yes      | The queue suffix for priority messages. Only orange box uses this field. Set it to `null`.                                                        |
| `varys_config_path`     | No       | The shared team Varys config. Do not change this value.                                                                                           |
| `varys_log_path`        | No       | The path of the Varys log file for the worker.                                                                                                    |

## Changing the config

If you change the config while the worker runs, the worker will log a warning and continue to use the old values. To apply the changes, restart the worker.

## Example

The file `devscape_my_cool_pipeline.json` below is a complete test config for an example pipeline, `my-cool-pipeline`.

```json
{
  "global": {
    "work_dir": "/shared/team/cherami_test/work",
    "output_dir": "/shared/team/cherami_test/outputs",
    "server": "devscape"
  },
  "pipeline": {
    "name": "my-cool-pipeline",
    "version": "v0.1.0",
    "path": "ukhsa-collaboration/my-cool-pipeline",
    "cpus": 4,
    "mem": "8G",
    "cpu_limit": 4,
    "mem_limit": "8G",
    "nf_config_path": "/shared/team/configs/nextflow/nextflow.config",
    "nf_profiles": [],
    "nf_extra_args": [
      "-e.ONYX_DOMAIN=$ONYX_DOMAIN",
      "-e.ONYX_TOKEN=$ONYX_TOKEN",
      "-latest"
    ],
    "namespace": "ns-ukhsa-devscape",
    "container": "quay.io/climb-tre/nextflow:25.04.8",
    "backoff_limit": 0,
    "max_attempts": 2,
    "retry_timeout": 10,
    "job_timeout": 3600
  },
  "worker": {
    "listen_exchange": "downstream-ukhsa_orange_box-devscape",
    "listen_queue_suffix": "ukhsa_mycoolpipeline_worker",
    "publish_queue_suffix": null,
    "publish_exchange": null,
    "rerun_queue_suffix": null,
    "rerun_exchange": null,
    "priority_queue_suffix": null,
    "priority_exchange": null,
    "varys_config_path": "/shared/team/configs/varys/varys.conf",
    "varys_log_path": "/shared/team/logs/varys/my_cool_pipeline_varys.log"
  }
}
```

With this config:

- Cherami will run `my-cool-pipeline` from the `v0.1.0` branch of `ukhsa-collaboration/my-cool-pipeline`, and query the `devscape` Onyx server.
- Cherami will write work files and outputs to test directories under `/shared/team/cherami_test/`.
- The worker will consume from the orange box exchange on the queue `ukhsa_mycoolpipeline_worker` and will not publish messages.
