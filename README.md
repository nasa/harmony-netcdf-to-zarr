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

### Docker-only

It is possible to develop and run this service locally using only Docker.  This is the recommended option
for validation and small changes.

#### Setup

Prerequisites:
  - Docker
  - This codebase

Copy [example/dotenv](example/dotenv) to `.env` (`cp example/dotenv .env`) and set variables according
to the instructions in the file.

#### Common tasks

Build new runtime and test images:
```
bin/build-image
bin/build-test-image
```

Run tests with coverage reports.  This will reflect any local changes made and in harmony-service-lib-py if it is checked
out into a peer directory
```
bin/test-in-docker
```

Run an example using the built Docker container and contents of [example/harmony-operation.json](example/harmony-operation.json)
as input.  This will reflect any local changes to the codebase and harmony-service-lib-py if it is checked out
into a peer directory.
```
bin/run-in-docker --harmony-action invoke --harmony-input "`cat example/harmony-operation.json`"
```

#### Setup

Prerequisites:
  - Python 3.7+, ideally installed via a virtual environment such as `pyenv`
  - Common compilers and build tools such as `gcc`, `g++`, and `make` as required
  - A local copy of the code
  - Localstack (optional, recommended)

Copy [example/dotenv](example/dotenv) to `.env` (`cp example/dotenv .env`) and set variables according
to the instructions in the file.

Install dependencies:
```
pip3 install -r requirements/core.txt -r requirements/dev.txt
```

#### Common tasks

Run tests with coverage reports:
```
bin/test
```

Run an example:
```
dotenv python -m harmony_netcdf_to_zarr --harmony-action invoke --harmony-input "`cat example/harmony-operation.json`"
```