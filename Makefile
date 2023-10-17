.PHONY: fix lint lint/ruff lint/mypy test

fix:
	ruff --fix .

lint: lint/ruff lint/mypy

lint/ruff:
	ruff .

lint/mypy:
	mypy r3 test

test:
	python -m pytest --cov=r3
