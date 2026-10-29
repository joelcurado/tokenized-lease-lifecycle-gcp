export PYTHONPATH := src

.PHONY: demo test
demo:      ## generate data, run the pipeline locally, reconcile, print the report
	python -m lease_demo.report
test:
	python -m pytest -q tests
