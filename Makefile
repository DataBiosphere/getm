include common.mk

MODULES=streaming_urls
SCRIPTS=scripts
tests:=$(wildcard tests/test_*.py)

test: lint mypy shared_memory_37 $(tests)
	coverage combine
	rm -f .coverage.*

# A pattern rule that runs a single test script
$(tests): %.py :
	coverage run -p --source=streaming_urls $*.py --verbose

lint:
	flake8 $(MODULES) $(SCRIPTS) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

benchmark:
	python tests/benchmark.py

version: streaming_urls/version.py

streaming_urls/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

build: clean version
	python setup.py bdist_wheel

shared_memory_37:
	python setup.py build_ext --inplace

sdist: clean version bgzip_utils.c
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: $(tests) benchmark streaming_urls/version.py clean build shared_memory_37 install
