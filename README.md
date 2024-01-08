# Harmony NetCDF4 to Zarr converter

A Harmony service to convert NetCDF4 files to Zarr files.  Takes conventional
Harmony messages and translates their input granules to Zarr using xarray.

This library intentionally does very little checking of the input files and
file extensions.  It is designed to work on NetCDF granules.  It ought to work
with any other file type that can be opened with
[xarray.open_mfdataset](http://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html)
using the `h5netcdf` driver.  This includes some HDF5 EOSDIS datasets.
Individual collections must be tested to ensure compatibility.


## Development Setup

### Harmony Instance

It is recommended that the NetCDF-to-Zarr service is tested and developed using
a local Harmony instance. This can be established following the instructions in
the [Harmony repository](https://github.com/nasa/harmony).

### Environment File

This service uses the
[harmony-service-lib-py](https://github.com/nasa/harmony-service-lib-py),
and requires that certain environment variables be set, as shown in the Harmony
Service Lib README. For example, `STAGING_BUCKET` and `STAGING_PATH` are
required, and `EDL_USERNAME` and `EDL_PASSWORD` are required for any
data behind Earthdata Login. For automated testing (not integrated into Harmony in
a dev environment or AWS deployment), use the example `.env` file in this repo:

    $ cp example/dotenv .env

and update the `.env` with the correct values.

### Python & Project Dependencies

In order to be able to run the automated tests, install Python (3.9), and create a Python virtual environment.

Activate the newly created environment and install the project dependencies:

    $ python -m pip install --upgrade pip
    $ make install 

## Running & Testing the Service

If using Minikube, be sure your environment is pointed to the Minikube Docker daemon:

    $ eval $(minikube docker-env)

Build the Docker image:

    $ make build-image

or build the image using a local copy of `harmony-service-lib-py` (useful for concurrent development):

    $ make target-name LOCAL_SVCLIB_DIR=../harmony-service-lib-py

You can now run a workflow in your local Harmony stack and it will execute using this image.

Restart the services in your local Harmony instance (the script below is
contained in the Harmony repository):

	$ bin/restart-services

Run through these steps again (build image, restart services) in order to pick up any new changes.

If you'd rather not build the image locally, you can simply pull the latest image: 
    
    $ docker pull ghcr.io/nasa/harmony-netcdf-to-zarr

## Automated Tests

Run tests with coverage reports:

    $ make test

You may be concurrently developing on this service as well as the `harmony-service-lib-py`. If so, and you
want to test changes to it along with this service, install the `harmony-service-lib-py` in 'development mode'.
Install it using pip and the path to the local clone of the service library:

```
pip install -e ../harmony-service-lib-py
```

Now any changes made to that local repo will be visible in this project when you run tests, etc.

## Contributions:

Developers working on the NetCDF-to-Zarr service will need to create a feature
branch for their work. The code in the repository has a `unittest` suite, which
should be updated when any code is added or updated within the repository.

When a feature branch is ready for review, a Pull Request (PR) should be opened
against the `main` branch. This will automatically trigger a GitHub workflow
that will run the `unittest` suite (see:
`.github/workflows/run_tests_on_pull_requests.yml`).

When a PR is merged against the `main` branch, a different workflow will check
if there are updates to the `version.txt` file. This file should contain a
semantic version number (see: `.github/workflows/publish_docker_image.yml`).

If there are updates to `version.txt`, the GitHub workflow will:

* Extract the semantic version number from that file.
* Extract the latest release notes from `CHANGELOG.md`.
* Run the `unittest` suite.
* Tag the most recent commit on the `main` branch with the semantic version
  number.
* Create a GitHub release using the release notes and semantic version number.
* Publish the NetCDF-to-Zarr service Docker image to ghcr.io. It will be tagged
  with the semantic version number.

For this reason, when releasing, please be sure to update both:

* version.txt
* CHANGELOG.md
