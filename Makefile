.PHONY: install test lint build-image push-image build-test-image test-in-docker run-in-docker

install:
	pip install -r requirements/core.txt -r requirements/dev.txt

test:
	bin/test

lint:
	flake8 --ignore=W503 harmony_netcdf_to_zarr

build-image:
	LOCAL_SVCLIB_DIR=${LOCAL_SVCLIB_DIR} bin/build-image

push-image:
	bin/push-image

build-test-image:
	bin/build-test-image

test-in-docker:
	LOCAL_SVCLIB_DIR=${LOCAL_SVCLIB_DIR} bin/test-in-docker

run-in-docker:
	LOCAL_SVCLIB_DIR=${LOCAL_SVCLIB_DIR} bin/run-in-docker example/harmony-operation.json
