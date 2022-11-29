"""R3 command line interface."""

import click

import r3
import r3.commit
import r3.init


@click.group()
@click.version_option(r3.__version__, message="%(version)s")
def cli() -> None:
    pass


cli.add_command(r3.commit.commit)
cli.add_command(r3.init.init)


if __name__ == "__main__":
    cli()
