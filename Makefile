.PHONY: clean doc doc-clean tests check test install


install:
	@which pip > /dev/null
	@pip freeze|grep 'pbsmrtpipe=='>/dev/null \
      && pip uninstall -y pbsmrtpipe \
      || echo -n ''
	@pip install ./
	@echo "Installed version pbsmrtpipe $(shell pbsmrtpipe --version)"

clean: doc-clean
	rm -rf build/;\
	find . -name "*.egg-info" | xargs rm -rf;\
	rm -rf dist/;\
	find . -name "*.pyc" | xargs rm -f;
	find . -name "job_output" | xargs rm -rf;
	rm -f nosetests.xml

doc:
	sphinx-apidoc -o docs/ pbsmrtpipe/ && cd docs/ && make html

doc-clean:
	rm -rf docs/pbsmrtpipe.*.rst
	rm -rf docs/modules.rst
	cd docs && make clean

unit-test:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/pb_tasks/tests/*.py pbsmrtpipe/tests/test_*.py

test-dev:
	cd testkit-data && fab cleaner && pbtestkit-multirunner --debug --nworkers 8 dev.fofn

test-unit:
	nosetests --verbose --with-xunit --logging-conf nose.cfg pbsmrtpipe/pb_tasks/tests/*.py pbsmrtpipe/tests/test_*.py

test-pipelines:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/tests/test_pb_pipelines_sanity.py

# This should probably go away
test-tasks:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/pb_tasks/tests/test_*.py

test-loader:
	python -c "import pbsmrtpipe.loader as L; L.load_all()"

test-contracts:
	python -c "import pbsmrtpipe.loader as L; L.load_all()"

test-chunk-operators:
	python -c "import pbsmrtpipe.loader as L; L.load_and_validate_chunk_operators()"

test-sanity: test-contracts test-pipelines test-chunk-operators test-loader

test-suite: test-sanity test-unit test-dev

test-clean-suite: install test-suite

clean-all:
	find . -name "*.pyc" | xargs rm -rf;\
	rm -rf report_unittests.log && cd testkit-data && fab cleaner

build-java-classes:
	avro-tools compile schema pbsmrtpipe/schemas java-classes/

write-pipeline-templates-avro:
	pbsmrtpipe show-templates --output-templates-avro extras/pipeline-templates-avro

write-pipeline-templates-json:
	pbsmrtpipe show-templates --output-templates-json extras/pipeline-templates-json

write-pipeline-templates: write-pipeline-templates-avro write-pipeline-templates-json

test-chunk:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/tests/test_tools_dev_tasks.py
