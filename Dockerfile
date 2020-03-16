# Debian-based Python.  Builds _significantly_ faster than alpine due to presence of pre-built binaries for most libs
FROM python:3.7.4-slim

WORKDIR "/home"

# Install static things necessary for building dependencies.
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y build-essential

# First run pip install against requirements.txt.  These take a while and are less likely to change than the service lib
COPY requirements/core.txt requirements/core.txt
RUN pip3 install -r requirements/core.txt

# Copy and install the service lib and its dependencies.
COPY deps deps
RUN pip3 install deps/harmony-service-lib-py

COPY . .

ENTRYPOINT ["python3", "-m", "harmony_netcdf_to_zarr"]
