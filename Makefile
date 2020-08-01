include common.mk

MODULES=streaming_urls

test: lint mypy tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=streaming_urls \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: streaming_urls/version.py

streaming_urls/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

build: clean version
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl

.PHONY: test lint mypy tests streaming_urls/version.py clean build install
