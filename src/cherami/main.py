from pathlib import Path

import rich_click as click

from cherami import PIGEON, __codename__, __version__, subcommands

version_codename = f"v{__version__}, codename {__codename__}"


def _print_version(
    click_context: click.Context,
    parameter: click.Parameter,
    value: bool,  # noqa: FBT001
) -> None:
    if not value:
        return

    click.echo(f"{click_context.info_name}, version {version_codename}")
    if parameter.name == "version":
        click.echo("\n" + PIGEON)
    click_context.exit()


@click.group()
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
@click.option(
    "--version",
    is_flag=True,
    expose_value=False,
    callback=_print_version,
)
@click.option(
    "-V",
    "version_short",
    is_flag=True,
    expose_value=False,
    callback=_print_version,
)
@click.pass_context
def cli(
    click_context: click.Context,
    log: Path | None,
    log_level: str,
) -> None:
    """Cherami command group."""
    click_context.ensure_object(dict)
    click_context.obj["log"] = log
    click_context.obj["log_level"] = log_level


cli.add_command(subcommands.serve)
cli.add_command(subcommands.describe)
cli.add_command(subcommands.evaluate)


if __name__ == "__main__":
    cli()
