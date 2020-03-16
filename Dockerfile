# Debian-based Python.  Builds _significantly_ faster than alpine due to presence of pre-built binaries for most libs
FROM python:3.7.4-slim as base

WORKDIR "/home"

# Multistage build https://docs.docker.com/develop/develop-images/multistage-build/
# "builder" will have the ability to build dependencies.  "base" will be the actual image
FROM base as builder

# Install static things necessary for building dependencies.
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y build-essential
RUN pip3 install cython

# First run pip install against requirements.txt.  These take a while and are less likely to change than the service lib
COPY requirements/core.txt requirements/core.txt
RUN pip3 install --prefix='/install' --no-warn-script-location -r requirements/core.txt

# Copy and install the service lib and its dependencies.
COPY deps deps
RUN pip3 install --prefix='/install' --no-warn-script-location deps/harmony-service-lib-py

# Main image.  Copy built dependencies and local files
FROM base

COPY --from=builder /install /usr/local
COPY . .

ENTRYPOINT ["python3", "-m", "harmony_netcdf_to_zarr"]
