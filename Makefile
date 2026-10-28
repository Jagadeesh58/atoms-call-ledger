.PHONY: install test lint format eval app check clean

PYTHON ?= python

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest -q

lint:
	ruff check .

format:
	ruff format --check .

eval:
	$(PYTHON) eval/run_eval.py --seed 7 --calls 1000

app:
	uvicorn app.main:app --reload --port 8000

check: lint format test
	bash scripts/check_repo.sh
	$(PYTHON) eval/run_eval.py --seed 7 --calls 1000
	git diff --exit-code eval/results/latest.json

clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
