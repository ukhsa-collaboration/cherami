import json
import logging
from pathlib import Path

import click

from cherami.config import CheramiConfig, load_config
from cherami.pipelines import load_pipeline_module
from cherami.utils import init_logging

logger = logging.getLogger(__name__)


@click.command(name="serve")
@click.option(
    "--audit_db",
    envvar="CHERAMI_AUDIT_DB",
    help="Path to audit SQLite database",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
)
@click.argument(
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
)
@click.pass_context
def serve(
    click_context: click.Context, config_path: Path, audit_db: Path
) -> None:
    log = click_context.obj["log"]
    log_level = click_context.obj["log_level"]
    init_logging(log, log_level)
    config: CheramiConfig = load_config(config_path)

    pipeline_module = load_pipeline_module(config.pipeline_config.name)
    worker_config = config.worker_config
    logger.debug("Worker config: %s", worker_config)
    worker_work_dir, worker_output_dir = config.pipeline_dirs()
    worker = pipeline_module.build_worker(
        worker_config,
        config.pipeline_config,
        worker_work_dir,
        worker_output_dir,
        audit_db_path=audit_db,
    )
    worker.run()


@click.command()
@click.argument(
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
)
@click.pass_context
def describe(click_context: click.Context, config_path: Path) -> None:
    config: CheramiConfig = load_config(config_path)
    descriptions = []

    worker_config = config.worker_config
    publish_queue_suffix = worker_config.publish_queue_suffix
    publish_exchange = (
        worker_config.publish_exchange or worker_config.listen_exchange
        if publish_queue_suffix
        else None
    )
    descriptions.append(
        {
            "name": config.pipeline_config.name,
            "listen_exchange": worker_config.listen_exchange,
            "listen_queue": worker_config.listen_queue_suffix,
            "publish_exchange": publish_exchange,
            "publish_queue": publish_queue_suffix,
        }
    )

    json_payload = {"workers": list(descriptions)}
    click.echo(json.dumps(json_payload, indent=2, sort_keys=False))


@click.command(name="evaluate")
@click.argument(
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
)
@click.argument("sample_ids", nargs=-1, required=True)
@click.pass_context
def evaluate(
    click_context: click.Context,
    config_path: Path,
    sample_ids: tuple[str, ...],
) -> None:
    log = click_context.obj["log"]
    log_level = click_context.obj["log_level"]
    init_logging(log, log_level)

    config = load_config(config_path)
    pipeline_module = load_pipeline_module(config.pipeline_config.name)
    pipeline = pipeline_module.build_pipeline(config.pipeline_config)

    results = {
        sample_id: pipeline.should_run(sample_id) for sample_id in sample_ids
    }
    click.echo(json.dumps(results, indent=2, sort_keys=False))
