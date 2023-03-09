import os

import yaml

with open("r3.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

name = config.get("parameters", dict()).get("name", "world")

os.makedirs("output", exist_ok=True)

with open("output/greeting", "w") as output_file:
    output_file.write(f"Hello, {name}!\n")
