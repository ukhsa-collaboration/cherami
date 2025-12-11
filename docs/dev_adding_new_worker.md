# Adding a new worker

This document describes how to add a new Cherami `Worker` so that a pipeline can be triggered from a message queue. It assumes the pipeline itself already exists in the config (see `docs/dev_adding_new_pipeline.md`).

## 1. Overview

- A worker listens to a RabbitMQ queue (via [Varys](https://github.com/CLIMB-TRE/varys)), receives messages, and runs a Cherami `Pipeline` (aka a Nextflow pipeline) for each sample.
- Worker behaviour is defined by:
  - A config entry under `"workers"`.
  - A Python subclass of `Worker`.
- The `Worker` class defines how to:
  - Listen to RabbitMQ exchanges for incoming samples
  - How to handle skipped, failed and sucessful samples
  - Publish to downstream exchanges based on events.
- Therefore each nextflow pipeline that will be run via cherami should have a `Worker` class implemented for it.
- Cherami reads worker settings from the provided config file to construct a `WorkerConfig` object which becomes a component of the `Worker` class.


## 2. Add the worker to the config file

Any config with the expected structure can be used via the `--config` CLI option or `CHERAMI_CONFIG` environment variable, allowing for different configurations for different environments (i.e synthscape vs mSCAPE). The repo contains configration (`./configs`) for common runtime environments already - therefore it is likely a new worker should be added to one of these.

Workers are configured under the top-level `"workers"` key in the Cherami config.

1. Open the appropriate config file for the environment you will be using (for example `configs/cherami_synthscape.json`).
2. Under `"workers"`, add a new object for the name of the worker name you are addign (for example `"my-worker"`).
3. Populate the required fields:

IMPORTANT:
- Pipeline names should NOT contain underscores (`_`) as these are NOT valid kuberentes job names.

```json
{
  "workers": {
    "my-worker": {
      "pipeline_name": "my-pipeline",
      "listen_exchange": "cherami_exchange",
      "listen_queue_suffix": "my_worker_queue",
      "publish_queue_suffix": null,
      "publish_exchange": null,
      "varys_config_path": "./conf/varys.cfg",
      "varys_log_path": "./my_worker_varys.log"
    }
  }
}
```

Expected fields are as follows:

| Field | Required | Description |
| --- | --- | --- |
| `pipeline_name` | Yes | Name of the pipeline the worker runs; must match an entry under `"pipelines"` in the same config file. |
| `listen_exchange` | Yes | RabbitMQ exchange the worker consumes from. |
| `listen_queue_suffix` | Yes | Queue suffix for this worker - varys combines this with the exchange name to form the queue name. |
| `publish_queue_suffix` | Optional | When set, successful runs publish the original message to this queue. The exchange defaults to `listen_exchange` unless `publish_exchange` is also provided. |
| `publish_exchange` | Optional | Exchange used for completion messages when `publish_queue_suffix` is set. |
| `varys_config_path` | Yes | Path to the Varys configuration file. |
| `varys_log_path` | Yes | Path to the Varys log file for this worker. |


## 3. Implement the Worker subclass

Workers are implemented in `src/cherami/workers`. Each worker is a subclass of `Worker`.

1. Create a new file `src/cherami/workers/my_worker.py`.
2. Implement a `Worker` subclass.

Example:

```python
from cherami.config import WorkerConfig
from cherami.pipelines import MyPipeline
from cherami.workers.worker import Worker


class MyWorker(Worker):
    def __init__(self, worker_config: WorkerConfig, pipeline: MyPipeline) -> None:
        super().__init__(worker_config=worker_config, pipeline=pipeline)
```

This is a minimal implementation that simply calls the parents constructor with the supplied config and pipeline. It will:

- Use the base `Worker` loop and retry logic.
- Run the `MyPipeline` pipeline for each message.

However workers can be configured with their own custom behaviour described below:

### 3.1. Optional: customise worker methods

The base `Worker` class defines several events you can override:

| Event | Trigger | Default behaviour |
| --- | --- | --- |
| `on_skip(message)` | `Worker.run` calls it when `pipeline.should_run(climb_id)` returns `False`. | Acknowledges the message and returns. |
| `on_success(message, payload)` | `Worker.run` calls it when `_runner.run_pipeline(...)` returns `result.success` `True`. | Optionally publish the parsed payload to `publish_queue_suffix` (if set), then acknowledge the message. |
| `on_retry(message)` | `Worker.run` calls it when `result.success` is `False`, `result.retry` is `True`, and retry attempts remain. | Negative acknowledge (nack) the message so it is requeued. |
| `on_sample_failure(message)` | `Worker.run` calls it when retries are exhausted, `result.retry` is `False`, or an unhandled exception occurs. | Acknowledge the message and log the failure. |

Override these methods in your `MyWorker` class if you need custom behaviour, for example to:

- Publish additional messages on success.
- Route failures to a separate queue.

## 4. Register the worker

Cherami looks up workers by name using the `WORKERS` mapping in `src/cherami/workers/__init__.py`.

1. Open `src/cherami/workers/__init__.py`.
2. Import your new worker class.
3. Add it to the `WORKERS` dictionary with the same key used in your config.

Example:

```python
from cherami.workers import amr, orange_box, my_worker
from cherami.workers.worker import Worker

WORKERS: dict[str, type[Worker]] = {
    "orange_box": orange_box.OrangeBoxWorker,
    "amr": amr.AmrWorker,
    "my-worker": my_worker.MyWorker,
}
```

The key `"my-worker"` must match the worker name in the config file (for example the key under `"workers"`).

IMPORTANT:
- Pipeline names should NOT contain underscores (`_`) as these are NOT valid kuberentes job names.

## 5. Running and testing the worker

Once the config and Python class are in place and registered:

1. Ensure your config file includes the new worker and refers to an existing pipeline.
2. Ensure that the pipeline name does not contain an underscore (`_`)
3. Write tests for any overridden behaviour.

There are some useful debug commands that can help testing new pipelines.

To check how workers are configured you can use `cherami describe` to inspect how workers are configured:

```bash
CHERAMI_CONFIG=path/to/config.json uv run cherami describe my-worker
```