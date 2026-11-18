# Glossary

Terms used throughout the Cherami documentation.

Some terms refer both to a concept and to a Python class. When refering the concept of a worker the guides use plain text (e.g worker) and when refering to the class, the guide uses backticked formatting (e.g `Worker`).

## Concepts

| Term | Description |
| --- | --- |
| Pipeline | A Nextflow pipeline that Cherami runs in a broad sense, e.g. AMR, defined by the `Pipeline` class. |
| Pathchar pipeline | A specific pathogen characterisation pipeline that always runs after orange box, defined by the `PathCharPipeline` class. |
| Worker | The Cherami process, started by `cherami serve` that consumes messages from a queue and coordinates runs of one pipeline for each sample, implemented by the `Worker` class. |
| Exchange | A RabbitMQ exchange that workers publish messages to and delivers a copy of each message to every queue bound to it. |
| Queue | A RabbitMQ queue bound to an exchange. |
| Queue suffix | A config value that Varys combines with the exchange name to make the queue name. |
| Upstream context | The `onyx_versions_hash` and `orange_box_version` for a sample, which together describe the upstream versions used for that sample. |
| onyx_versions_hash | A hash of the upstream tool and database versions held in Onyx for a sample. |
| Trace file | A report file from Nextflow that Cherami uses to check the exit code of each process. |

## Python classes

| Class | Description |
| --- | --- |
| `Pipeline` | Abstract base class in `pipelines/pipeline.py` that defines a Nextflow pipeline for use in Cherami, e.g. samplesheet generation, `should_run` logic and success criteria. |
| `PathCharPipeline` | Subclass of `Pipeline` that adds the default version check against Onyx, and the usual base class for new pipelines. |
| `Worker` | Base class in `pipelines/worker.py` containing the logic for consuming messages, running the pipeline and publishing results. |
| `PipelineContext` | Holds the sample ID, job UUID, payload and upstream context for one message. |
