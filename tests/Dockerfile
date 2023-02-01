ARG TAG=latest
FROM ghcr.io/nasa/harmony-netcdf-to-zarr:$TAG

COPY requirements/core.txt requirements/core.txt

RUN pip3 install -r requirements/core.txt -r requirements/dev.txt

ENTRYPOINT ["bash", "bin/test"]
