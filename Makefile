.PHONY: install test lint

install:
	python -m pip install -r requirements.txt || true

test:
	python -m pytest -q

lint:
	python -m flake8 src tests || true
