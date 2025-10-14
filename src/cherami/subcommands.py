import json
import logging
import multiprocessing
import signal
import uuid
from collections.abc import Sequence
from multiprocessing.synchronize import Event
from pathlib import Path

import click

from cherami.pipeline_runner import PipelineRunner
from cherami.pipelines import PIPELINES
from cherami.utils import init_kubernetes, init_logging, setup_queue_logging
from cherami.workers import WORKERS
from cherami.workers.base import Worker

logger = logging.getLogger(__name__)


def _run_worker_process(
    worker_class: type[Worker],
    sample_log: Path,
    shutdown_event: Event,
    log_queue: multiprocessing.Queue,
    log_level: str,
) -> None:
    ## since we are using spawn to create a new process, multiprocessing pickles and moves everything into the new
    ## process. Thus the mp target is a wrapper function that creates a worker instance, rather than using the actual
    ## run method from a worker object directly. This is done to ensure that the spawned process will have the worker
    ## instance created within it
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    setup_queue_logging(log_queue, log_level)

    ## type is ignored here as the worker_class is a subclass of BaseWorker, and subclasses override __init__
    ## with no args (instead supplying them via super())
    worker = worker_class()  # type: ignore
    worker.run(sample_log, shutdown_event)


def _launch_workers(
    worker_classes: Sequence[tuple[str, type[Worker]]],
    sample_log: Path,
    log_queue: multiprocessing.Queue,
    log_level: str,
) -> None:
    context = multiprocessing.get_context("spawn")
    shutdown_event = context.Event()

    processes = []
    try:
        for worker_name, worker_class in worker_classes:
            process = context.Process(
                name=f"cherami-worker-{worker_name}",
                target=_run_worker_process,
                args=(worker_class, sample_log, shutdown_event, log_queue, log_level),
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

    selected_workers = [(name, WORKERS[name]) for name in worker_names] if worker_names else list(WORKERS.items())
    log_queue, log_process = init_logging(log, log_level)
    sample_log.parent.mkdir(parents=True, exist_ok=True)

    try:
        _launch_workers(
            worker_classes=selected_workers, sample_log=sample_log, log_queue=log_queue, log_level=log_level
        )
    finally:
        log_queue.put(None)
        log_process.join(timeout=10)


@click.command()
@click.argument(
    "worker_names", nargs=-1, metavar="WORKER_NAMES", type=click.Choice(list(WORKERS.keys()), case_sensitive=False)
)
def describe(worker_names: tuple[str, ...]) -> None:
    selected_workers = [WORKERS[name] for name in worker_names] if worker_names else list(WORKERS.values())
    descriptions = []

    for worker in selected_workers:
        worker_instance = worker()  # type: ignore

        publish_queue_suffix = worker_instance.publish_queue_suffix
        publish_exchange = (
            worker_instance.publish_exchange or worker_instance.listen_exchange if publish_queue_suffix else None
        )
        descriptions.append(
            {
                "name": worker_instance.worker_name,
                "listen_exchange": worker_instance.listen_exchange,
                "listen_queue": worker_instance.listen_queue_suffix,
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
                pipeline = PIPELINES[pipeline_name]()
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
            pipeline = PIPELINES[pipeline_name]()
            should_run = pipeline.should_run(sample_id)
            click.echo(f"{sample_id}\t{pipeline_name}\t{should_run}")
