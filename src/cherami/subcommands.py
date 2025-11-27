import json
import logging
import multiprocessing
import signal
import uuid
from collections.abc import Sequence
from multiprocessing.synchronize import Event
from pathlib import Path

import click

from cherami.config import load_pipeline_config, load_worker_config
from cherami.pipeline_runner import PipelineRunner
from cherami.pipelines import PIPELINES
from cherami.utils import init_kubernetes, init_logging, setup_queue_logging
from cherami.workers import WORKERS, Worker

logger = logging.getLogger(__name__)


def _run_worker_process(
    worker_cls: type[Worker],
    worker_name: str,
    raw_config: dict,
    sample_log: Path,
    shutdown_event: Event,
    log_queue: multiprocessing.Queue,
    log_level: str,
) -> None:
    ## entry point for worker process that contstructs the worker object within the spawned process
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    setup_queue_logging(log_queue, log_level)

    worker_config = load_worker_config(worker_name, raw_config)
    pipeline_config = load_pipeline_config(worker_config.pipeline_name, raw_config)
    if worker_config.pipeline_name not in PIPELINES:
        raise ValueError(f"Invalid pipeline '{worker_config.pipeline_name}' for worker '{worker_name}'")

    ## create a pipeline instance for the worker based on name given in config
    pipeline = PIPELINES[worker_config.pipeline_name](pipeline_config)
    ## and run
    worker = worker_cls(worker_config, pipeline)
    worker.run(sample_log, shutdown_event)


def _launch_workers(
    worker_classes: Sequence[tuple[str, type[Worker]]],
    raw_config: dict,
    sample_log: Path,
    log_queue: multiprocessing.Queue,
    log_level: str,
) -> None:
    context = multiprocessing.get_context("spawn")
    shutdown_event = context.Event()

    processes = []
    try:
        for worker_name, worker_cls in worker_classes:
            process = context.Process(
                name=f"cherami-worker-{worker_name}",
                target=_run_worker_process,
                args=(
                    worker_cls,
                    worker_name,
                    raw_config,
                    sample_log,
                    shutdown_event,
                    log_queue,
                    log_level,
                ),
            )
            process.start()
            processes.append(process)

        while True:
            for process in processes:
                if not process.is_alive():
                    logger.error("worker process %s exited unexpectedly, exit code %s", process.name, process.exitcode)
                    raise RuntimeError(f"worker process {process.name} exited unexpectedly")
    except KeyboardInterrupt:
        logger.info("stopping workers...")
    finally:
        shutdown_event.set()
        for process in processes:
            process.join(timeout=10)


@click.command()
@click.argument(
    "worker_names", nargs=-1, metavar="WORKER_NAMES", type=click.Choice(list(WORKERS.keys()), case_sensitive=False)
)
@click.pass_context
def spawn(click_context: click.Context, worker_names: tuple[str, ...]) -> None:
    sample_log = click_context.obj["sample_log"]
    log = click_context.obj["log"]
    log_level = click_context.obj["log_level"]
    config = click_context.obj["config"]

    selected_workers = [(name, WORKERS[name]) for name in worker_names] if worker_names else list(WORKERS.items())
    log_queue, log_process = init_logging(log, log_level)
    sample_log.parent.mkdir(parents=True, exist_ok=True)

    try:
        _launch_workers(
            worker_classes=selected_workers,
            raw_config=config,
            sample_log=sample_log,
            log_queue=log_queue,
            log_level=log_level,
        )
    finally:
        log_queue.put(None)
        log_process.join(timeout=10)


@click.command()
@click.argument(
    "worker_names", nargs=-1, metavar="WORKER_NAMES", type=click.Choice(list(WORKERS.keys()), case_sensitive=False)
)
@click.pass_context
def describe(click_context: click.Context, worker_names: tuple[str, ...]) -> None:
    config = click_context.obj["config"]
    selected_workers = [(name, WORKERS[name]) for name in worker_names] if worker_names else list(WORKERS.items())
    descriptions = []

    for worker_name, worker_cls in selected_workers:
        worker_config = load_worker_config(worker_name, config)
        pipeline_config = load_pipeline_config(worker_config.pipeline_name, config)
        if worker_config.pipeline_name not in PIPELINES:
            raise ValueError(f"Invalid pipeline '{worker_config.pipeline_name}' for worker '{worker_name}'")

        pipeline = PIPELINES[worker_config.pipeline_name](pipeline_config)
        worker = worker_cls(worker_config, pipeline)

        publish_queue_suffix = worker.publish_queue_suffix
        publish_exchange = worker.publish_exchange or worker.listen_exchange if publish_queue_suffix else None
        descriptions.append(
            {
                "name": worker.worker_name,
                "listen_exchange": worker.listen_exchange,
                "listen_queue": worker.listen_queue_suffix,
                "publish_exchange": publish_exchange,
                "publish_queue": publish_queue_suffix,
            }
        )

    json_payload = {"workers": list(descriptions)}
    click.echo(json.dumps(json_payload, indent=2, sort_keys=False))


@click.command()
@click.argument("sample_ids", nargs=-1, required=True)
@click.option(
    "--pipelines",
    required=True,
    help="Comma-separated list of pipelines to run",
    type=click.STRING,
)
@click.pass_context
def run(
    click_context: click.Context,
    sample_ids: tuple[str, ...],
    pipelines: str,
) -> None:
    sample_log = click_context.obj["sample_log"]
    log = click_context.obj["log"]
    log_level = click_context.obj["log_level"]
    config = click_context.obj["config"]
    if pipelines:
        pipeline_names = [p.strip() for p in pipelines.split(",")]
        invalid = [p for p in pipeline_names if p not in PIPELINES]
        if invalid:
            raise click.BadParameter(
                f"Invalid pipeline(s): {', '.join(invalid)}. Valid options: {', '.join(PIPELINES.keys())}"
            )
    else:
        pipeline_names = PIPELINES.keys()

    log_queue, log_process = init_logging(log, log_level)
    sample_log.parent.mkdir(parents=True, exist_ok=True)
    try:
        k8_api = init_kubernetes()
        pipeline_runner = PipelineRunner(k8_api=k8_api, sample_log=sample_log)
        for sample_id in sample_ids:
            for pipeline_name in pipeline_names:
                pipeline_config = load_pipeline_config(pipeline_name, config)
                pipeline = PIPELINES[pipeline_name](pipeline_config)  # type: ignore[arg-type]
                logger.info("Processing %s with %s pipeline", sample_id, pipeline_name)
                if not pipeline.should_run(sample_id):
                    logger.info("Skipping %s for %s", sample_id, pipeline_name)
                    continue
                try:
                    pipeline.validate()
                except Exception as e:
                    logger.error("Pipeline validation failed for %s: %s", pipeline_name, e)
                    continue
                job_uuid = str(uuid.uuid4())
                try:
                    result = pipeline_runner.run_pipeline(
                        pipeline=pipeline,
                        sample_id=sample_id,
                        job_uuid=job_uuid,
                    )
                    if result.success:
                        logger.info("%s completed %s successfully", sample_id, pipeline_name)
                    else:
                        logger.error("%s failed %s: %s", sample_id, pipeline_name, ", ".join(result.errors))
                except Exception as e:
                    logger.error("Exception running %s for %s: %s", pipeline_name, sample_id, e)

    finally:
        log_queue.put(None)
        log_process.join(timeout=10)


@click.command()
@click.argument("sample_ids", nargs=-1, required=True)
@click.option(
    "--pipelines",
    required=False,
    help="Comma-separated list of pipelines to evaluate (if not specified, evaluates all)",
    type=click.STRING,
)
def evaluate(
    sample_ids: tuple[str, ...],
    pipelines: str | None,
) -> None:
    raw_config = click.get_current_context().obj["config"]
    if pipelines:
        pipeline_names = [p.strip() for p in pipelines.split(",")]
        invalid = [p for p in pipeline_names if p not in PIPELINES]
        if invalid:
            raise click.BadParameter(
                f"Invalid pipeline(s): {', '.join(invalid)}. Valid options: {', '.join(PIPELINES.keys())}"
            )
    else:
        pipeline_names = PIPELINES.keys()

    for sample_id in sample_ids:
        for pipeline_name in pipeline_names:
            pipeline_config = load_pipeline_config(pipeline_name, raw_config)
            pipeline = PIPELINES[pipeline_name](pipeline_config)  # type: ignore[arg-type]
            should_run = pipeline.should_run(sample_id)
            click.echo(f"{sample_id}\t{pipeline_name}\t{should_run}")
