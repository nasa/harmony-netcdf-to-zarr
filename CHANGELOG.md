## v1.0.3
### 2022-12-13

* DAS-1713 - Publish Docker images to ghcr.io.
* DAS-1712 - Disable DockerHub publication actions.
* DAS-1695 - Ensure correct metadata is written to store.
* Give `compute_chunksize` a side effect to prevent pickling error in testing end to end.
* Check multiprocess exit codes.
* DAS-1685 - Ensure single granule requests don't attempt aggregation.
* DAS 1673 - Implement smart chunking.
* DAS-1536 - Ensure raised exception messages are correctly tested.
* HARMONY-1189 - Update harmony-service-lib dependency to 1.0.20.
* HARMONY-1178 - Handle paged STAC input
* DAS-1438 - Ensure zero-dimensional variables, like a CRS, resolve full paths.
* DAS-1438 - Ensure Zarr groups always use a `ProcessSynchronizer`.
* DAS-1455 - Copy exact data from NetCDF-4 input to Zarr store, including scale and offset metadata.
* DAS-1379 - Only aggregate temporal dimensions.
* DAS-1432 - Ensure only the Zarr store is in the STAC.
* DAS-1379 - Write many input NetCDF-4 granules to single Zarr store.
* DAS-1414 - Check input granule is NetCDF-4 via media type or extension.
* DAS-1400 - Support bounds during dimension aggregation.
* DAS-1376 - Update `HarmonyAdapter` to perform many-to-one operations.
* DAS-1375 - Scale dimension values to integers before finding differences in resolution calculation.
* DAS-1375 - Add dimension aggregation to `DimensionsMapping` class.
* DAS-1374 - Add NetCDF-4 dimension parsing classes, ready for aggregation.

## v1.0.2
### 2021-11-29

* Add trigger on release publish.
* HARMONY-388 - Make publish-image consistent with service-example.
* Performance and chunk sizing improvements (HARMONY-953, HARMONY-953, HARMONY-992, HARMONY-877, HARMONY-855).

## v1.0.1
### 2021-06-17

* HARMONY-388 - Improve consistency across Python repositories.

## v1.0.0
### 2021-05-17

* HARMONY-817 - Change occurrences of "harmony" to "harmonyservices" for Docker images.
* HARMONY-817 - Publish to DockerHub on merge and release.
* HARMONY-816 - Add user agent logging.
