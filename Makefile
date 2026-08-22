.PHONY: install lint typecheck test snapshot backfill api clean

install:
	uv venv -q && uv pip install -q -e '.[dev]'

lint:
	ruff check src tests scripts && ruff format --check src tests scripts

typecheck:
	mypy src/chainpulse

test:
	pytest -q

snapshot:
	python scripts/snapshot.py snapshot

backfill:
	python scripts/snapshot.py backfill

api:
	uvicorn chainpulse.api:app --reload

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache data
