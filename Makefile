.PHONY: test lint check format

test:
	python -m unittest discover -s tests -p 'aggregator_test.py'
	python -m unittest discover -s tests -p 'end_to_end_test.py'
	python -m unittest discover -s tests -p 'athena_test.py'
	python -m unittest discover -s tests -p 'helpers_test.py'
	python -m unittest discover -s tests -p 'processor_test.py'

lint:
	@ruff --fix cmoncrawl tests || ( echo ">>> ruff failed"; exit 1; )

format:
	@pre-commit run --all-files

check: format lint
