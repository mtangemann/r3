.PHONY: black isort lint lint/black lint/flake8 lint/isort lint/mypy test

black:
	black r3 test

isort:
	isort r3 test

lint: lint/black lint/flake8 lint/isort lint/mypy

lint/black:
	black --check r3 test

lint/flake8:
	flake8 r3 test

lint/isort:
	isort --check r3 test

lint/mypy:
	mypy r3 test

test:
	python -m pytest --cov=r3
