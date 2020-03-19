ARG TAG=latest
FROM harmony/netcdf-to-zarr:$TAG

COPY requirements/core.txt requirements/core.txt

RUN pip3 install -r requirements/dev.txt

ENTRYPOINT ["bash", "bin/test"]
