ARG TAG=latest
FROM harmonyservices/netcdf-to-zarr:$TAG

COPY requirements/core.txt requirements/core.txt

RUN pip3 install -r requirements/core.txt -r requirements/dev.txt

ENTRYPOINT ["bash", "bin/test"]
