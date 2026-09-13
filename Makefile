.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8
	flake8 pipelime tests

test: ## run tests quickly with the default Python
	pytest

PYTEST := .venv/bin/python -m pytest -o addopts="" -p no:cacheprovider

test-tier0: ## pydantic-dense modules + contract tests (~15s), run after every change
	$(PYTEST) -q tests/pipelime/utils tests/pipelime/stages tests/pipelime/piper tests/pipelime/sequences tests/pipelime/choixe tests/pipelime/test_pydantic_contract.py --deselect tests/pipelime/sequences/test_grabber.py --ignore tests/pipelime/piper/progress

test-tier1: ## tier0 + cli + a commands slice (1-3 min), subtask gate
	$(PYTEST) -q tests/pipelime/utils tests/pipelime/stages tests/pipelime/piper tests/pipelime/sequences tests/pipelime/choixe tests/pipelime/cli tests/pipelime/test_pydantic_contract.py tests/pipelime/commands/test_interfaces.py tests/pipelime/commands/test_pipe.py tests/pipelime/commands/test_map.py tests/pipelime/commands/test_split.py --ignore tests/pipelime/piper/progress

test-full: ## whole suite in parallel (xdist, per-file groups)
	$(PYTEST) -q -n auto --dist loadgroup tests

test-warnfree: ## tier0 with pydantic deprecation warnings as errors (final check)
	$(PYTEST) -q -W error::pydantic.PydanticDeprecatedSince20 tests/pipelime/utils tests/pipelime/stages tests/pipelime/piper tests/pipelime/sequences tests/pipelime/choixe tests/pipelime/test_pydantic_contract.py --deselect tests/pipelime/sequences/test_grabber.py --ignore tests/pipelime/piper/progress

coverage: ## check code coverage quickly with the default Python
	coverage run --source pipelime -m pytest
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -rf docs/_static/generated
	rm -rf docs/api/generated
	sphinx-apidoc -o docs/api/generated pipelime
#	python docs/pl_help.py
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .
