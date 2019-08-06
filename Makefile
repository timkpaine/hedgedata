tests: clean ## Clean and Make unit tests
	python3 -m pytest -v tests --cov=hedgedata

test: clean lint ## run the tests for travis CI
	@ python3 -m pytest -v tests --cov=hedgedata

lint: ## run linter
	pylint hedgedata || echo
	flake8 hedgedata 

annotate: ## MyPy type annotation check
	mypy -s hedgedata

annotate_l: ## MyPy type annotation check - count only
	mypy -s hedgedata | wc -l 

clean: ## clean the repository
	find . -name "__pycache__" | xargs  rm -rf 
	find . -name "*.pyc" | xargs rm -rf 
	rm -rf .coverage cover htmlcov logs build dist *.egg-info

build:  ## build the repository
	python3 setup.py build

install:  ## install to site-packages
	python3 setup.py install

dist:  ## dist to pypi
	rm -rf dist build
	python3 setup.py sdist
	python3 setup.py bdist_wheel
	twine check dist/* && twine upload dist/*

# Thanks to Francoise at marmelab.com for this
.DEFAULT_GOAL := help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

print-%:
	@echo '$*=$($*)'

.PHONY: clean run test tests help annotate annotate_l docs run build js dist
