include common.mk

MODULES=getm
SCRIPTS=scripts
tests:=$(wildcard tests/test_*.py)

test: lint mypy shared_memory_37 $(tests)
	coverage combine
	rm -f .coverage.*

# A pattern rule that runs a single test script
$(tests): %.py :
	coverage run -p --source=getm $*.py --verbose

lint:
	flake8 $(MODULES) $(SCRIPTS) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

benchmark:
	python tests/benchmark.py

version: getm/version.py

getm/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

build: clean version
	python setup.py bdist_wheel

shared_memory_37:
	python setup.py build_ext --inplace

sdist: clean version
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: $(tests) benchmark getm/version.py clean build shared_memory_37 install
