from logging import Logger
from multiprocessing import Manager, Process, Queue
from multiprocessing.managers import Namespace
from os import cpu_count, environ
from os.path import splitext
from queue import Empty as QueueEmpty
from re import findall
from time import time
from typing import Any, List, Set, Tuple, Union, Dict, MutableMapping

from fsspec.mapping import FSMap
from netCDF4 import Dataset, Group as NetCDFGroup, Variable as NetCDFVariable
from s3fs import S3FileSystem
from zarr import (DirectoryStore, group as create_zarr_group,
                  Group as ZarrGroup, ProcessSynchronizer)
from zarr.core import Array as ZarrArray
from zarr.convenience import consolidate_metadata
import numpy as np

from harmony_netcdf_to_zarr.mosaic_utilities import DimensionsMapping, resolve_reference_path
from harmony_netcdf_to_zarr.process_utilities import monitor_processes

# Types for function signatures
Number = Union[np.integer, np.floating, int, float]
ZarrStore = Union[DirectoryStore, FSMap]

# Some global variables that may be shared by different methods
region = environ.get('AWS_DEFAULT_REGION') or 'us-west-2'
# This dictionary converts from a string representation of units, such as
# kibibytes, mebibytes or gibibytes, to a raw number of bytes. This is used
# when a compressed chunk size is expressed as a string. See the NIST standard
# for binary prefix: https://physics.nist.gov/cuu/Units/binary.html.
binary_prefix_conversion_map = {'Ki': 1024, 'Mi': 1048576, 'Gi': 1073741824}


def make_localstack_s3fs() -> S3FileSystem:
    host = environ.get('LOCALSTACK_HOST') or 'host.docker.internal'
    return S3FileSystem(
        use_ssl=False,
        key='ACCESS_KEY',
        secret='SECRET_KEY',
        client_kwargs=dict(
            region_name=region,
            endpoint_url=f'http://{host}:4572'))


def make_s3fs() -> S3FileSystem:
    return S3FileSystem(client_kwargs=dict(region_name=region))


def mosaic_to_zarr(input_granules: List[str], zarr_store: Union[FSMap, str],
                   process_count: int = None, logger: Logger = None):
    """ Convert input NetCDF files to a Zarr store, preserving data, metadata
        and group hierarchy.

        This function makes use of multiple processes, to parallelise the
        output of multiple granules. The number of processes will be the
        smallest option of:

        * The number available CPUs.
        * The number of input granules.
        * The number of requested processes.

        Parameters:
        ___________
        input_granules: a list of file paths for local NetCDF-4 files to
            convert.
        zarr_store: a location on disc into which a zarr.DirectoryStore will
            be written or a MutableMapping into which Zarr data can be written.
            This MutableMapping object points at the S3 object that represents
            the root of the Zarr store, which is essentially a directory in
            the S3 bucket.
        logger: a Logger to include performance information

    """
    t1 = time()
    if isinstance(zarr_store, str):
        zarr_store = DirectoryStore(zarr_store)

    # Write dimension information from DimensionsMapping (including bounds)
    # Store list of aggregated dimensions/variables to avoid writing them again
    # Aggregation will only occur for multiple granule requests that set
    # `concatenate=true`.
    dim_mapping = DimensionsMapping(input_granules)
    variable_chunk_metadata = granule_chunk_shapes(input_granules[0])

    aggregated_dimensions = __copy_aggregated_dimensions(
        dim_mapping, zarr_store, variable_chunk_metadata
    )

    if process_count is None:
        process_count = min(cpu_count(), len(input_granules))
    else:
        process_count = min(process_count, cpu_count(), len(input_granules))

    with Manager() as manager:
        output_queue = manager.Queue(len(input_granules))
        shared_namespace = manager.Namespace()

        if isinstance(zarr_store, DirectoryStore):
            shared_namespace.store_type = 'DirectoryStore'
            shared_namespace.zarr_root = zarr_store.dir_path()
        else:
            shared_namespace.store_type = 'S3FileSystem'
            shared_namespace.zarr_root = zarr_store.root

        for input_granule in input_granules:
            output_queue.put(input_granule)

        processes = [Process(target=_output_worker,
                             args=(output_queue, shared_namespace,
                                   aggregated_dimensions, dim_mapping,
                                   variable_chunk_metadata))
                     for _ in range(process_count)]

        monitor_processes(processes, shared_namespace,
                          error_notice='Problem writing data to Zarr store')

    _finalize_metadata(zarr_store)
    t2 = time()
    logger.info(f'ZarrStore filled in {(t2-t1):.4f}s, N_GRANULES: {len(input_granules)}, '
                f'N_PROCESSES:{process_count}, sec/gran: {(t2-t1)/len(input_granules):.4f}')

    try:
        zarr_store.close()
    except AttributeError:
        pass


def _finalize_metadata(store: MutableMapping) -> None:
    """Safely prepare a store for consolidated metadata reading.

    This function forces a "flush" of the input store before calling zarr's
    consolidate_metadata function.  This ensures that whatever store handle is
    passed to this routine has the most up to date information before
    consolidation.

    """
    store['.zforceflush'] = b'Temp file to force store sync.'
    del store['.zforceflush']
    consolidate_metadata(store)


def _output_worker(output_queue: Queue, shared_namespace: Namespace,
                   aggregated_dimensions: Set[str], dim_mapping: DimensionsMapping,
                   variable_chunk_metadata: Dict = {}) -> None:
    """ This worker function is executed in a spawned process. It checks for
        items in the main queue, which correspond to local file paths for input
        NetCDF-4 files. If there is at least one URL left for writing, then the
        groups, variables and attributes from that NetCDF-4 file are written to
        the output Zarr store, in the related slice of the aggregated output
        array.

    """
    if shared_namespace.store_type == 'S3FileSystem':
        if environ.get('USE_LOCALSTACK') == 'true':
            s3 = make_localstack_s3fs()
        else:
            s3 = make_s3fs()

        zarr_store = s3.get_mapper(root=shared_namespace.zarr_root,
                                   check=False, create=True)
    else:
        zarr_store = DirectoryStore(shared_namespace.zarr_root)

    zarr_synchronizer = ProcessSynchronizer(
        f'{splitext(shared_namespace.zarr_root)[0]}.sync'
    )

    while (not hasattr(shared_namespace, 'exception')
           and not output_queue.empty()
           and not hasattr(shared_namespace, 'process_error')):
        try:
            input_granule = output_queue.get_nowait()
        except QueueEmpty:
            break

        try:
            with Dataset(input_granule, 'r') as input_dataset:
                input_dataset.set_auto_maskandscale(False)
                __copy_group(input_dataset,
                             create_zarr_group(zarr_store, overwrite=False,
                                               synchronizer=zarr_synchronizer),
                             dim_mapping, aggregated_dimensions,
                             variable_chunk_metadata)
        except Exception as exception:
            # If there was an issue, save a string message from the raised
            # exception. This will cause the other processes to stop processing
            # input NetCDF-4 files.
            shared_namespace.exception = str(exception)
            raise exception


def __copy_aggregated_dimensions(dim_mapping: DimensionsMapping,
                                 zarr_store: ZarrStore,
                                 variable_chunk_metadata: Dict) -> Set[str]:
    """ Iterate through all aggregated dimensions, and their associated bounds,
        and write these dimensions to the output Zarr store. A list of
        aggregated dimensions are retained, so that the data values are not
        overwritten when writing all other variables.

        To limit this function to the scope of TRT-121, only aggregated
        temporal dimensions are considered. The aggregated spacial information
        exists in the `DimensionsMapping` instance, so the temporal check could
        be removed to allow all aggregated, 1-D grid dimensions to be inserted
        into the Zarr store.

    """
    if isinstance(zarr_store, DirectoryStore):
        zarr_root = zarr_store.dir_path()
    else:
        zarr_root = zarr_store.root

    zarr_synchronizer = ProcessSynchronizer(f'{splitext(zarr_root)[0]}.sync')
    root_group = create_zarr_group(zarr_store, overwrite=True,
                                   synchronizer=zarr_synchronizer)
    aggregated_dimensions = set()

    for output_dimension in dim_mapping.output_dimensions.values():
        if output_dimension.is_temporal():
            __copy_aggregated_dimension(output_dimension.dimension_path,
                                        output_dimension.values, root_group,
                                        variable_chunk_metadata)
            aggregated_dimensions.add(output_dimension.dimension_path)

            if output_dimension.bounds_path is not None:
                __copy_aggregated_dimension(output_dimension.bounds_path,
                                            output_dimension.bounds_values,
                                            root_group,
                                            variable_chunk_metadata)
                aggregated_dimensions.add(output_dimension.bounds_path)

    return aggregated_dimensions


def __copy_aggregated_dimension(variable_path: str, variable_data: np.ndarray,
                                root_group: ZarrGroup,
                                variable_chunk_metadata: Dict) -> None:
    """ This function will copy variable data, but not metadata, from the
        supplied variable array. Metadata attributes will be later added when
        all variables are iterated through within each granule.

        Technically, this function is used for both aggregated dimensions and
        any associated bounds variables.

    """
    variable_path_pieces = variable_path.lstrip('/').split('/')
    variable_basename = variable_path_pieces.pop()
    parent_group = root_group

    for nested_group in variable_path_pieces:
        parent_group = parent_group.require_group(nested_group)

    chunks = (variable_chunk_metadata.get(variable_path)
              or compute_chunksize(variable_data.shape, variable_data.dtype))

    parent_group.require_dataset(variable_basename,
                                 data=variable_data,
                                 shape=variable_data.size,
                                 chunks=tuple(chunks),
                                 dtype=variable_data.dtype)


def __copy_group(netcdf_group: NetCDFGroup, zarr_group: ZarrGroup,
                 dim_mapping: DimensionsMapping,
                 aggregated_dimensions: Set[str] = set(),
                 variable_chunk_metadata: Dict = {}):
    """ Recursively copies the source netCDF4 group into the destination Zarr
        group, along with all sub-groups, variables, and attributes. The input
        `zarr_group` has an associated `ProcessSynchronizer` object, which
        allows for writing data to the same object from within parallel
        processes. This object is automatically propagated to child groups and
        datasets via the `require_group` and `require_dataset` class methods.

        Parameters
        ----------
        netcdf_group : netCDF4.Group
            the NetCDF group to copy from
        zarr_group : zarr.hierarchy.Group
            the existing Zarr group to copy into
        dim_mapping: mosaic_utilities.DimensionsMapping
            contains aggregated dimension values and units metadata
        aggregated_dimensions: Set[str]
            a set of full string paths of aggregated dimensions. As of TRT-121
            these are only temporal dimensions (and associated bounds variables)
        variable_chunk_metadata: Dict
            A Dict of fully qualified variable names keys holding
            the output chunksizes

    """
    __copy_attrs(netcdf_group, zarr_group)

    for child_group_name, child_netcdf_group in netcdf_group.groups.items():
        __copy_group(child_netcdf_group,
                     zarr_group.require_group(child_group_name.split('/').pop()),
                     dim_mapping, aggregated_dimensions, variable_chunk_metadata)

    for variable_name, netcdf_variable in netcdf_group.variables.items():
        __copy_variable(netcdf_variable, zarr_group, variable_name,
                        dim_mapping, aggregated_dimensions, variable_chunk_metadata)


def __copy_variable(netcdf_variable: NetCDFVariable, zarr_group: ZarrGroup,
                    variable_name: str, dim_mapping: DimensionsMapping,
                    aggregated_dimensions: Set[str] = set(),
                    variable_chunk_metadata: Dict = {}) -> None:
    """ Copies the variable from the NetCDF variable into the Zarr group,
        giving it the provided variable_name. Note, the `Group.require_dataset`
        class method instantiates a dataset that uses the `ProcessSynchronizer`
        associated with the group, so that the dataset can be safely written to
        from within multiple processes.

        Parameters
        ----------
        netcdf_variable : netCDF4.Variable
            the source variable to copy
        zarr_group : zarr.hierarchy.Group
            the group into which to copy the variable
        variable_name : string
            the variable_name of the variable in the destination group
        dim_mapping: DimensionsMapping
            an object containing aggregated dimension information.
        aggregated_dimensions: Set[str]
            a set of all dimension variable names that have been aggregated.
            This ensures that the aggregated data will not be overwritten by
            the input source data.
        variable_chunk_metadata: Dict
            A Dict of fully qualified variable names keys holding
            the output chunksizes

    """
    resolved_variable_name = resolve_reference_path(netcdf_variable,
                                                    variable_name)
    # create zarr group/dataset
    chunks = netcdf_variable.chunking()
    if chunks == 'contiguous' or chunks is None:
        chunks = netcdf_variable.shape

    if not chunks and len(netcdf_variable.dimensions) == 0:
        # Treat a 0-dimensional NetCDF variable as a zarr group
        zarr_variable = zarr_group.require_group(variable_name)
    else:

        # Derive the aggregated shape, used for both the chunk size calculation
        # and as the shape of the output Zarr variable.
        aggregated_shape = __get_aggregated_shape(netcdf_variable, dim_mapping,
                                                  aggregated_dimensions)

        fill_value = getattr(netcdf_variable, '_FillValue', 0)

        chunks = (variable_chunk_metadata.get(resolved_variable_name)
                  or compute_chunksize(netcdf_variable.shape, netcdf_variable.dtype))

        zarr_variable = zarr_group.require_dataset(
            variable_name,
            shape=aggregated_shape,
            chunks=tuple(chunks),
            dtype=netcdf_variable.dtype,
            fill_value=fill_value)

        if resolved_variable_name not in aggregated_dimensions:
            # For a non-aggregated dimension, insert input granule data
            __insert_data_slice(netcdf_variable, zarr_variable,
                                resolved_variable_name, dim_mapping)

    # xarray requires the _ARRAY_DIMENSIONS metadata to know how to label axes
    kwarg_attributes = {'_ARRAY_DIMENSIONS': list(netcdf_variable.dimensions)}

    # If the variable has been aggregated, the units must be used from the
    # aggregated dimension (or bounds) variable.
    if resolved_variable_name in aggregated_dimensions:
        if resolved_variable_name in dim_mapping.output_dimensions:
            aggregated_units = (
                dim_mapping.output_dimensions[resolved_variable_name].units
            )
        elif resolved_variable_name in dim_mapping.output_bounds:
            dimension_path = dim_mapping.output_bounds[resolved_variable_name]
            aggregated_units = (
                dim_mapping.output_dimensions[dimension_path].units
            )

        kwarg_attributes['units'] = aggregated_units

    __copy_attrs(netcdf_variable, zarr_variable, **kwarg_attributes)


def __get_aggregated_shape(netcdf_variable: NetCDFVariable,
                           dim_mapping: DimensionsMapping,
                           aggregated_dimensions) -> Tuple[int]:
    """ Derive the output array shape for a given input NetCDF-4 variable.
        There are several possible use-cases:

        * An input variable matches an aggregated dimension, so the output
          dimension shape should be used instead of the input dimension shape.
        * An input variable matches an aggregated bounds variable, so the
          output bounds shape should be used instead of the input variable
          shape.
        * An input variable has a dimension that has been aggregated (e.g., a
          temporal dimension). For this dimension, the array size should match
          the length of the aggregated temporal dimension. Unaggregated
          dimension sizes should still match the input.
        * An input variable has no aggregated dimensions. The output variable
          shape should match in the input variable shape.

    """
    variable_path = resolve_reference_path(netcdf_variable,
                                           netcdf_variable.name)

    if variable_path in aggregated_dimensions:
        if variable_path.endswith('_bnds'):
            dim_path = variable_path[:-5]
            aggregated_shape = dim_mapping.output_dimensions[dim_path].bounds_values.shape
        else:
            aggregated_shape = dim_mapping.output_dimensions[variable_path].values.shape
    else:
        aggregated_shape = []
        for dim_index, dim_name in enumerate(netcdf_variable.dimensions):
            dimension_path = resolve_reference_path(netcdf_variable, dim_name)

            if dimension_path in aggregated_dimensions:
                aggregated_shape.append(
                    dim_mapping.output_dimensions[dimension_path].values.size
                )
            else:
                aggregated_shape.append(netcdf_variable.shape[dim_index])

    return tuple(aggregated_shape)


def __insert_data_slice(netcdf_variable: NetCDFVariable, zarr_variable: ZarrArray,
                        variable_name: str, dim_mapping: DimensionsMapping):
    """ A helper function that identifies the index ranges in the aggregated
        output dimension into which the input values from the NetCDF-4
        variable should be inserted, and then updates the output Zarr store
        with these data.

    """
    netcdf_file_path = netcdf_variable.group().filepath()

    dimension_indices = []

    for dimension in netcdf_variable.dimensions:
        dimension_path = resolve_reference_path(netcdf_variable, dimension)

        if (
                dimension_path in dim_mapping.output_dimensions
                and dim_mapping.output_dimensions[dimension_path].is_temporal()
        ):
            output_dimension = dim_mapping.output_dimensions[dimension_path]
            input_dimension = (dim_mapping.input_dimensions[dimension_path]
                                                           [netcdf_file_path])
            input_values = input_dimension.get_values(output_dimension.units)
            output_indices = np.where(np.in1d(output_dimension.values,
                                              input_values))[0]

            # This assumes that all input grid values from a single granule
            # represent a continuous segment of the output dimension. Also,
            # `slice(m, n)` will address from element `m`, up to (but not
            # including) `n`, so need to extend the upper end of the slice by 1
            # to include all input data.
            dimension_indices.append(slice(output_indices.min(),
                                           output_indices.max() + 1))
        else:
            # Currently only supporting temporal aggregation. Other dimensions,
            # such as spatial dimensions, are assumed to be identical in all
            # input granules.
            dimension_indices.append(slice(None))

    zarr_variable[tuple(dimension_indices)] = netcdf_variable[:]


def __copy_attrs(netcdf_input: Union[NetCDFVariable, NetCDFGroup],
                 zarr_output: Union[ZarrGroup, ZarrArray], **kwargs):
    """ Copies all attributes from the source group or variable into the
        destination group or variable. If the Zarr store already has that
        attribute, it is not overwritten. For example, the units attribute of
        aggregated temporal dimensions.

         Converts netCDF4 variable values from their native type (typically
         Numpy dtypes) into JSON-serializable values that Zarr can store

        Parameters
        ----------
        netcdf_input : netCDF4.Group | netCDF4.Variable
            The source from which to copy attributes
        zarr_output : zarr.hierarchy.Group | zarr.core.Array
            The destination into which to copy attributes.
        **kwargs : dict
            Additional attributes to add to the destination

    """
    existing_attributes = set(zarr_output.attrs.keys())
    new_attributes = {key: __netcdf_attr_to_python(getattr(netcdf_input, key))
                      for key in netcdf_input.ncattrs()}

    new_attributes.update(kwargs)

    for existing_attribute in existing_attributes:
        new_attributes.pop(existing_attribute, None)

    zarr_output.attrs.update(new_attributes)


def __netcdf_attr_to_python(val: Any) -> Any:
    """
    Given an attribute value read from a NetCDF file (typically a numpy type),
    returns the value as a Python primitive type, e.g. np.integer -> int.

    Returns the value unaltered if it does not need conversion or is unrecognized

    Parameters
    ----------
    val : any
        An attribute value read from a NetCDF file

    Returns
    -------
    any
        The converted value
    """
    if isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.floating):
        return float(val)
    elif isinstance(val, np.ndarray):
        return [__netcdf_attr_to_python(v) for v in val.tolist()]
    elif isinstance(val, bytes):
        # Assumes bytes are UTF-8 strings.  This holds for attributes.
        return val.decode('utf-8')
    else:
        return val


def compute_chunksize(shape: Union[tuple, list],
                      datatype: str,
                      compression_ratio: float = 1.5,
                      compressed_chunksize_byte: Union[int, str] = '10 Mi'):
    """
    Compute the chunksize for a given shape and datatype
        based on the compression requirement
    We will try to make it equal along different dimensions,
        without exceeding the given shape boundary

    Parameters
    ----------
    shape : list/tuple
        the zarr shape
    datatype: str
        the zarr data type
            which must be recognized by numpy
    compression_ratio: str
        expected compression ratio for each chunk
        default to 7.2 which is the compression ratio
            from a chunk size of (3000, 3000) with double precision
            compressed to 10 Mi
    compressed_chunksize_byte: int/string
        expected chunk size in bytes after compression
        If it's a string, assuming it follows NIST standard for binary prefix
            (https://physics.nist.gov/cuu/Units/binary.html)
            except that only Ki, Mi, and Gi are allowed.
        Space is optional between number and unit.

    Returns
    -------
    list/tuple
        the regenerated new zarr chunks
    """
    # convert compressed_chunksize_byte to integer if it's a str
    if type(compressed_chunksize_byte) == str:
        try:
            (value, unit) = findall(
                r'^\s*([\d.]+)\s*(Ki|Mi|Gi)\s*$', compressed_chunksize_byte
            )[0]
        except IndexError:
            raise ValueError('Chunksize needs to be either an integer or '
                             'string. If it\'s a string, assuming it follows '
                             'NIST standard for binary prefix '
                             '(https://physics.nist.gov/cuu/Units/binary.html)'
                             ' except that only Ki, Mi, and Gi are allowed.')

        compressed_chunksize_byte = int(float(value)) * int(binary_prefix_conversion_map[unit])

    # get product of chunksize along different dimensions before compression
    if compression_ratio < 1.:
        raise ValueError('Compression ratio < 1 found when estimating chunk size.')
    chunksize_unrolled = int(
        compressed_chunksize_byte * compression_ratio / np.dtype(datatype).itemsize
    )

    # compute the chunksize by trying to make it equal along different dimensions,
    #    without exceeding the given shape boundary
    suggested_chunksize = np.full(len(shape), 0)
    shape_array = np.array(shape)
    dim_to_process = np.full(len(shape), True)
    while not (~dim_to_process).all():
        chunksize_remaining = chunksize_unrolled // suggested_chunksize[~dim_to_process].prod()
        chunksize_oneside = int(pow(chunksize_remaining, 1 / dim_to_process.sum()))
        if (shape_array[dim_to_process] >= chunksize_oneside).all():
            suggested_chunksize[dim_to_process] = chunksize_oneside
            dim_to_process[:] = False
        else:
            dim_to_fill = dim_to_process & (shape_array < chunksize_oneside)
            suggested_chunksize[dim_to_fill] = shape_array[dim_to_fill]
            dim_to_process[dim_to_fill] = False

    # return new chunks
    suggested_chunksize = type(shape)(suggested_chunksize.tolist())
    return suggested_chunksize


def granule_chunk_shapes(granule_filename: str) -> Dict:
    """Compute chunk sizes for all variables in a granule file.

    Traverse an input granule file, computing the chunksize for every found
    variable, and return a dictionary of the resulting chunksizes stored by
    their fully qualified variable name.

    """
    with Dataset(granule_filename) as dataset:
        return _chunk_shapes(dataset)


def _chunk_shapes(group: Union[Dataset, NetCDFGroup], results: Dict = {}) -> Dict:
    """Recursively return chunk shapes for all variables from group."""
    for netcdf4_variable in group.variables.values():
        variable_path = '/'.join([group.path, netcdf4_variable.name])
        variable_path = f'/{variable_path.lstrip("/")}'
        results[variable_path] = compute_chunksize(netcdf4_variable.shape, netcdf4_variable.dtype)

    for child_group in group.groups.values():
        results.update(_chunk_shapes(child_group, results))

    return results
