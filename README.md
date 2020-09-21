# harmony/netcdf-to-zarr

A Harmony service to convert NetCDF4 files to Zarr files.  Takes conventional Harmony messages and translates
their input granules to Zarr using xarray.

This library intentionally does very little checking of the input files and file extensions.  It is designed
to work on NetCDF granules.  It ought to work with any other file type that can be opened with
[xarray.open_mfdataset](http://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html) using the
`h5netcdf` driver.  This includes some HDF5 EOSDIS datasets.  Individual collections must be tested to ensure
compatibility.


## Development

### Setup

#### Docker

It is possible to develop and run this service locally using only Docker.  This is the recommended option
for validation and small changes. Install [Docker](https://www.docker.com/get-started) on your development
machine.

#### Environment file

This service uses the 
[harmony-service-lib-py](https://git.earthdata.nasa.gov/projects/HARMONY/repos/harmony-service-lib-py/browse), 
and requires that certain environment variables be set, as shown in the Harmony Service Lib README. For example,
`STAGING_BUCKET` and `STAGING_PATH` are required, and `EDL_USERNAME` and `EDL_PASSWORD` are required for any
data behind Earthdata Login. For local testing (not integrated into Harmony in a dev environment or AWS
deployment), use the example `.env` file in this repo:

    $ cp example/dotenv .env

and update the `.env` with the correct values.

#### Python & Project Dependencies (Optional)

If you would like to do local development outside of Docker, install Python, create a Python virtualenv, 
and install the project dependencies.

If you have [pyenv](https://github.com/pyenv/pyenv) and
[pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) installed (recommended), install Python and
create a virtualenv:

    $ pyenv install 3.7.4
    $ pyenv virtualenv 3.7.4 harmony-ntz
    $ pyenv activate harmony-ntz
    $ pyenv version > .python-version

The last step above creates a local .python-version file which will be automatically activated when cd'ing into the
directory if pyenv-virtualenv has been initialized in your shell (See the pyenv-virtualenv docs linked above).

Install project dependencies:

    $ pip install -r requirements/core.txt -r requirements/dev.txt

NOTE: All steps in this README which install dependencies need to be performed while on the NASA VPN
in order to download and install the Harmony Service Lib, which is published on the
[Nexus artifact repository](https://maven.earthdata.nasa.gov/).

### Development with Docker

#### Testing & Running the Service Independently

To run unit tests, coverage reports, or run the service on a sample message _outside_ of the 
entire Harmony stack, start by building new runtime and test images:

*IMPORTANT*: Be sure to do these steps in a shell in which has *not* been updated to point to
the Minikube Docker daemon. This is usually done via a shell `eval` command. Doing so will 
cause tests and the service to fail due to limitations in Minikube.

    $ bin/build-image
    $ bin/build-test-image

Run unit tests and generate overage reports. This will mount the local directory into the 
container and run the unit tests. So all tests will reflect local changes to the service.

    $ bin/test-in-docker

You may be concurrently making changes to the Harmony Service Lib. To run the unit tests using
the local clone of that Harmony Service Lib and any changes made to it:

    $ LOCAL_SVCLIB_DIR=../harmony-service-lib-py bin/test-in-docker

Finally, run the service using an example Harmony operation request 
([example/harmony-operation.json](example/harmony-operation.json) as input.  This will reflect
local changes to this repo, but will not include local changes to the Harmony Service Lib.

    $ bin/run-in-docker example/harmony-operation.json

To run the example and also include local Harmony Service Lib changes:

    $ LOCAL_SVCLIB_DIR=../harmony-service-lib-py bin/run-in-docker example/harmony-operation.json

#### Testing & Running the Service in Harmony

*Without local Harmony Service Lib changes*:

Be sure your environment is pointed to the Minikube Docker daemon:

    $ eval $(minikube docker-env)

Build the image:

    $ bin/build-image

You can now run a workflow in your local Harmony stack and it will execute using this image.

*With local Harmony Service Lib changes*:

To run this service in Harmony *with* a local copy of the Service Lib, build
the image, but specify the location of the local Harmony Service Lib clone:

    $ LOCAL_SVCLIB_DIR=../harmony-service-lib-py bin/build-image

You can now run a workflow in your local Harmony stack and it will execute using this image.
Note that this also means that the image needs to be rebuilt (using the same command) to
include any further changes to the Harmony Service Lib or this service.

### Development without Docker

#### Testing & running the Service Independently

Run tests with coverage reports:

    $ bin/test

Run an example:

    $ dotenv run python3 -m harmony_netcdf_to_zarr --harmony-action invoke --harmony-input "`cat example/harmony-operation.json`"

#### Installing `harmony-service-lib-py` in Development Mode

You may be concurrently developing on this service as well as the `harmony-service-lib-py`. If so, and you 
want to test changes to it along with this service, install the `harmony-service-lib-py` in 'development mode'. 
Install it using pip and the path to the local clone of the service library:

```
pip install -e ../harmony-service-lib-py
```

Now any changes made to that local repo will be visible in this project when you run tests, etc.

Finally, you can test & run the service in Harmony just as shown in the `Development with Docker` section above.
