"""R3 command line interface."""

import click

import r3


@click.group()
@click.version_option(r3.__version__, message="%(version)s")
def cli() -> None:
    pass


if __name__ == "__main__":
    cli()
