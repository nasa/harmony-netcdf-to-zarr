.PHONY: install test lint build-image

install:
	pip install -r requirements/core.txt -r requirements/dev.txt

test:
	bin/test

lint:
	flake8 --ignore=W503 harmony_netcdf_to_zarr

build-image:
	LOCAL_SVCLIB_DIR=${LOCAL_SVCLIB_DIR} bin/build-image