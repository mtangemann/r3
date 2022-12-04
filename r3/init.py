import os
from pathlib import Path

import click
import yaml

import r3


@click.command()
@click.argument("path", type=click.Path(file_okay=False, exists=False, path_type=Path))
def init(path: Path):
    print(f"Initializing empty repository in {path}")

    os.makedirs(path)
    os.makedirs(path / "data")
    os.makedirs(path / "jobs" / "by_hash")

    r3config = {"version": r3.__version__}

    with open(path / "r3.yaml", "w") as config_file:
        yaml.dump(r3config, config_file)
