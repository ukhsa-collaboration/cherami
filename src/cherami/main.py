from pathlib import Path

import click

from cherami import subcommands


@click.group()
@click.option(
    "--sample-log",
    help="Path to JSONL file for per-pipeline results",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("./sample_log.jsonl"),
)
@click.option(
    "--log",
    help="Path to log file (if not specified, logs to stderr)",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
)
@click.option(
    "--log-level",
    help="Logging level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
)
@click.pass_context
def cli(click_context: click.Context, sample_log: Path, log: Path | None, log_level: str) -> None:
    """Cherami command group."""
    click_context.ensure_object(dict)
    click_context.obj["sample_log"] = sample_log
    click_context.obj["log"] = log
    click_context.obj["log_level"] = log_level


cli.add_command(subcommands.spawn)
cli.add_command(subcommands.run)
cli.add_command(subcommands.describe)
cli.add_command(subcommands.evaluate)


if __name__ == "__main__":
    cli()
