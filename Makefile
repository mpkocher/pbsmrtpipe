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
	cd docs && make clean

unit-test:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/pb_tasks/tests pbsmrtpipe/tests

test-dev:
	cd testkit-data && fab cleaner && pbtestkit-multirunner --debug --nworkers 4 dev.fofn

test-unit:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/pb_tasks/tests pbsmrtpipe/tests

test-pipelines:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/tests/test_pb_pipelines_sanity.py

test-tasks:
	nosetests --verbose --logging-conf nose.cfg pbsmrtpipe/pb_tasks/tests/test_*.py

test-suite: test-unit test-dev

test-clean-suite: install test-suite

clean-all:
	rm -rf report_unittests.log && cd testkit-data && fab cleaner
