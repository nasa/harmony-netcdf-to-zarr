ARG TAG=latest
FROM harmony/netcdf-to-zarr:$TAG

COPY requirements/core.txt requirements/core.txt

RUN pip3 install -r requirements/core.txt -r requirements/dev.txt
