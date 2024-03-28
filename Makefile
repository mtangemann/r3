.PHONY: fix lint lint/ruff lint/mypy test

fix:
	ruff check --fix .

lint: lint/ruff lint/mypy

lint/ruff:
	ruff check .

lint/mypy:
	mypy r3 test migration

test:
	python -m pytest --cov=r3
