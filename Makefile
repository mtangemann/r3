.PHONY: fix lint ruff mypy test

fix:
	ruff check --fix .

lint: ruff mypy

ruff:
	pre-commit run ruff --all-files

mypy:
	mypy r3 test migration

test:
	python -m pytest --cov=r3
