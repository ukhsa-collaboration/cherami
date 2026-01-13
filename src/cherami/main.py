from pathlib import Path

import click

from cherami import subcommands


@click.group()
@click.option(
    "--audit_db",
    envvar="CHERAMI_AUDIT_DB",
    help="Path to audit SQLite database",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--log",
    help="Path to log file (if not specified, logs to stderr)",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
)
@click.option(
    "--log_level",
    help="Logging level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False
    ),
    default="INFO",
)
@click.pass_context
def cli(
    click_context: click.Context,
    audit_db: Path,
    log: Path | None,
    log_level: str,
) -> None:
    """Cherami command group."""
    click_context.ensure_object(dict)
    click_context.obj["audit_db"] = audit_db
    click_context.obj["log"] = log
    click_context.obj["log_level"] = log_level


cli.add_command(subcommands.serve)
cli.add_command(subcommands.describe)
cli.add_command(subcommands.evaluate)


if __name__ == "__main__":
    cli()
