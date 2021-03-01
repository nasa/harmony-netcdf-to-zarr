# Debian-based Python.  Builds _significantly_ faster than alpine due to presence of pre-built binaries for most libs
FROM python:3.7.4-slim

WORKDIR /opt/harmony-netcdf-to-zarr

# Install static things necessary for building dependencies.
RUN pip3 install --upgrade pip
RUN apt-get update && apt-get install -y build-essential git

# Install Python dependencies
COPY requirements/core.txt requirements/core.txt
RUN pip3 install -r requirements/core.txt

COPY . .

ENTRYPOINT ["python3", "-m", "harmony_netcdf_to_zarr"]
