.PHONY: test lint check format

test:
	python -m unittest discover -s tests -p '*test.py'

lint:
	@ruff --fix cmoncrawl tests || ( echo ">>> ruff failed"; exit 1; )

format:
	@pre-commit run --all-files

check: format lint
