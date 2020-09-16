# harmony/netcdf-to-zarr

A Harmony service to convert NetCDF4 files to Zarr files.  Takes conventional Harmony messages and translates
their input granules to Zarr using xarray.

This library intentionally does very little checking of the input files and file extensions.  It is designed
to work on NetCDF granules.  It ought to work with any other file type that can be opened with
[xarray.open_mfdataset](http://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html) using the
`h5netcdf` driver.  This includes some HDF5 EOSDIS datasets.  Individual collections must be tested to ensure
compatibility.

## Environment

Uses variables as defined in
[harmony-service-lib-py](https://git.earthdata.nasa.gov/projects/HARMONY/repos/harmony-service-lib-py/browse)

`STAGING_BUCKET` and `STAGING_PATH` are required. `EDL_USERNAME` and `EDL_PASSWORD` are
required for any data behind Earthdata Login

## Development

### Development with Docker

It is possible to develop and run this service locally using only Docker.  This is the recommended option
for validation and small changes.

#### Setup

Prerequisites:
  - Docker

Copy [example/dotenv](example/dotenv) to `.env` (`cp example/dotenv .env`) and set variables according
to the instructions in the file.

#### Common tasks

NOTE: All steps which install dependencies need to be performed while on the NASA VPN.

Build new runtime and test images:
```
bin/build-image
bin/build-test-image
```

Run tests with coverage reports.  This will reflect local changes to this repo.
```
bin/test-in-docker
```

Run tests using a local repo for the Harmony Service Library:
```
LOCAL_SVCLIB_DIR=/path/to/harmony-service-lib-py bin/test-in-docker
```

Run an example using the built Docker container and contents of [example/harmony-operation.json](example/harmony-operation.json)
as input.  This will reflect local changes to this repo, as well as any made in
harmony-service-lib-py if it has been installed in 'development mode' (see below).
```
bin/run-in-docker example/harmony-operation.json
```

As with the tests, you can run using a local repo for the Harmony Service Library:
```
LOCAL_SVCLIB_DIR=/path/to/harmony-service-lib-py bin/run-in-docker example/harmony-operation.json
```

NOTE: The steps above that use a local clone of the Harmony Service Library repo don't copy
the contents of that local repo into the image. Instead, they mount the local directory
into the running container so that any changes in the repo are reflected in the container.
So, if you'd like to build a Docker image that *does* have the contents of the local Harmony
Service Library repo copied into it, as you did above, specify its location when
building the image:

```
LOCAL_SVCLIB_DIR=/path/to/harmony-service-lib-py bin/build-image
```

Now the local harmony/netcdf-to-zarr:latest Docker image will contain the local version
of both the Harmony Service Library and this service. This also means that the image
needs to be rebuilt (using the same command) to test changes to this service or the
Harmony Service Library.

### Development without Docker

#### Setup

Prerequisites:
  - Python 3.7+, ideally installed via a virtual environment such as `pyenv`
  - Common compilers and build tools such as `gcc`, `g++`, and `make` as required
  - A local copy of the code
  - Localstack (optional, recommended)
Optional:
  - [harmony-service-lib-py](https://git.earthdata.nasa.gov/projects/HARMONY/repos/harmony-service-lib-py/browse) checked out in a peer directory

Copy [example/dotenv](example/dotenv) to `.env` (`cp example/dotenv .env`) and set variables according
to the instructions in the file.

NOTE: Installing dependencies must be done while connected to the NASA VPN.

If you have [pyenv](https://github.com/pyenv/pyenv) and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) installed (recommended),
install Python and create a virtualenv:

```
pyenv install 3.7.4
pyenv virtualenv 3.7.4 harmony-ntz
pyenv activate harmony-ntz
pyenv version > .python-version
```

The last step above creates a local .python-version file which will be automatically activated when cd'ing into the
directory if pyenv-virtualenv has been initialized in your shell (See the pyenv-virtualenv docs linked above).

Install project dependencies:
```
pip install -r requirements/core.txt -r requirements/dev.txt
```

### Installing `harmony-service-lib-py` in Development Mode

You may be concurrently developing on this service as well as the `harmony-service-lib-py`. If so, and you 
want to test changes to it along with this service, install the `harmony-service-lib-py` in 'development mode'. 
Install it using pip and the path to the local clone of the service library:

```
pip install -e /path/to/harmony-service-lib-py
```

Now any changes made to that local repo will be visible in this project when you run tests, etc.

#### Common tasks

Run tests with coverage reports:
```
bin/test
```

Run an example:
```
dotenv run python3 -m harmony_netcdf_to_zarr --harmony-action invoke --harmony-input "`cat example/harmony-operation.json`"
```
