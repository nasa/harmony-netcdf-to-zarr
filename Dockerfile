# Debian-based Python.  Builds _significantly_ faster than alpine due to presence of pre-built binaries for most libs
FROM python:3.9.14-slim-bullseye

WORKDIR /opt/harmony-netcdf-to-zarr

# Install static things necessary for building dependencies.
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y build-essential git

# Install Python dependencies
COPY requirements/core.txt requirements/core.txt
RUN pip3 install -r requirements/core.txt

# This is below the preceding layer to prevent Docker from rebuilding the
# previous layer (forcing a reload of dependencies) whenever the
# status of a local service library changes
ARG service_lib_dir=NO_SUCH_DIR

# Install a local harmony-service-lib-py if we have one
COPY deps ./deps/
RUN if [ -d deps/${service_lib_dir} ]; then echo "Installing from local copy of harmony-service-lib"; pip install -e deps/${service_lib_dir}; fi

COPY . .

ENTRYPOINT ["python3", "-m", "harmony_netcdf_to_zarr"]
