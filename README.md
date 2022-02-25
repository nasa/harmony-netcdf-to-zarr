# Harmony NetCDF4 to Zarr converter

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
[harmony-service-lib-py](https://github.com/nasa/harmony-service-lib-py),
and requires that certain environment variables be set, as shown in the Harmony Service Lib README. For example,
`STAGING_BUCKET` and `STAGING_PATH` are required, and `EDL_USERNAME` and `EDL_PASSWORD` are required for any
data behind Earthdata Login. For local testing (not integrated into Harmony in a dev environment or AWS
deployment), use the example `.env` file in this repo:

    $ cp example/dotenv .env

and update the `.env` with the correct values.

#### Python & Project Dependencies (Optional)

If you would like to do local development outside of Docker, install Python (3.7.4), and create a Python virtual environment.

Install project dependencies:

    $ python -m pip install --upgrade pip
    $ make install

### Development with Docker

If you'd rather not build the image locally (as instructed below), you can simply pull the latest image: 
    
    $ docker pull harmonyservices/netcdf-to-zarr

Some of the [Makefile](./Makefile) targets referenced below include an optional argument that allows us to use a local copy of 
`harmony-service-lib-py` (which is useful for concurrent development): 
    
    $ make target-name LOCAL_SVCLIB_DIR=../harmony-service-lib-py

#### Testing & Running the Service Independently

To run unit tests, coverage reports, or run the service on a sample message _outside_ of the
entire Harmony stack, start by building new runtime and test images:

*IMPORTANT*: If Minikube is installed, be sure to do these steps in a shell in which has *not* been updated to point to
the Minikube Docker daemon. This is usually done via a shell `eval` command. Doing so will
cause tests and the service to fail due to limitations in Minikube.

    $ make build-image
    $ make build-test-image

Run unit tests and generate overage reports. This will mount the local directory into the
container and run the unit tests. So all tests will reflect local changes to the service.

    $ make test-in-docker

Finally, run the service using an example Harmony operation request
([example/harmony-operation.json](example/harmony-operation.json)) as input.  This will reflect
local changes to this repo, but will not include local changes to the Harmony Service Lib.

    $ make run-in-docker

#### Testing & Running the Service in Harmony

*Without local Harmony Service Lib changes*:

If using Minikube, be sure your environment is pointed to the Minikube Docker daemon:

    $ eval $(minikube docker-env)

Build the image:

    $ make build-image

You can now run a workflow in your local Harmony stack and it will execute using this image.

### Development without Docker

#### Testing & running the Service Independently

This will require credentials for the Harmony Sandbox NGAPShApplicationDeveloper
to be present in your `~/.aws/credentials` file.

Run tests with coverage reports:

    $ make test

Run an example:

    $ dotenv run python3 -m harmony_netcdf_to_zarr --harmony-action invoke --harmony-input "$(bin/replace.sh example/harmony-operation.json)"

#### Installing `harmony-service-lib-py` in Development Mode

You may be concurrently developing on this service as well as the `harmony-service-lib-py`. If so, and you
want to test changes to it along with this service, install the `harmony-service-lib-py` in 'development mode'.
Install it using pip and the path to the local clone of the service library:

```
pip install -e ../harmony-service-lib-py
```

Now any changes made to that local repo will be visible in this project when you run tests, etc.

Finally, you can test & run the service in Harmony just as shown in the `Development with Docker` section above.
