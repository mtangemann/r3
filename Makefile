.PHONY: black isort lint lint/black lint/flake8 lint/isort lint/mypy

black:
	black r3

isort:
	isort r3

lint: lint/black lint/flake8 lint/isort lint/mypy

lint/black:
	black --check r3

lint/flake8:
	flake8 r3

lint/isort:
	isort --check r3

lint/mypy:
	mypy r3
