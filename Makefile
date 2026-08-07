.PHONY: install format lint typecheck test check run

install:
	uv sync --group dev

format:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .

typecheck:
	uv run mypy src tests

test:
	uv run pytest

check: lint typecheck test

run:
	uv run meeting-orchestrator serve
