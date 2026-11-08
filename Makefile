.PHONY: install lint fmt test test-int test-all

install:
	pip install -e ".[dev,dashboard]"

lint:
	ruff check .
	ruff format --check .

fmt:
	ruff check --fix .
	ruff format .

test:
	pytest -q -m "not integration"

test-int:
	pytest -q -m integration

test-all:
	pytest -q
