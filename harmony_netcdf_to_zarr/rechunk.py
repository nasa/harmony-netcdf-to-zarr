"""Code that will rechunk an existing zarr store."""
from __future__ import annotations

from harmony_netcdf_to_zarr.convert import compute_chunksize

from fsspec.mapping import FSMap
from time import time
from rechunker import rechunk
from typing import List, Dict, TYPE_CHECKING
if TYPE_CHECKING:
    from harmony_netcdf_to_zarr.adapter import NetCDFToZarrAdapter
from zarr import open_consolidated, consolidate_metadata, Group as zarrGroup
import xarray as xr


def rechunk_zarr(zarr_root: str, adapter: NetCDFToZarrAdapter) -> str:
    """Rechunks the zarr store found at zarr_root location.

    It creates a new rechunked store and removes the store found at zarr_root.

    """
    temp_root = zarr_root.replace('.zarr', '_tmp.zarr')
    target_root = zarr_root.replace('.zarr', '_rechunked.zarr')

    zarr_store = adapter.s3.get_mapper(root=zarr_root,
                                       check=False,
                                       create=False)

    zarr_temp = adapter.s3.get_mapper(root=temp_root, check=False, create=True)
    zarr_target = adapter.s3.get_mapper(root=target_root,
                                        check=False,
                                        create=True)

    try:
        adapter.s3.rm(temp_root, recursive=True)
    except FileNotFoundError:
        adapter.logger.info(f'Nothing to clean in {temp_root}')

    try:
        adapter.s3.rm(target_root, recursive=True)
    except FileNotFoundError:
        adapter.logger.info(f'Nothing to clean in {target_root}')

    t1 = time()
    rechunk_zarr_store(zarr_store, zarr_target, zarr_temp)
    t2 = time()
    adapter.logger.info(f'Function rechunk_zarr_store executed in {(t2-t1):.4f}s')

    adapter.s3.rm(zarr_root, recursive=True)
    adapter.s3.rm(temp_root, recursive=True)

    return target_root


def rechunk_zarr_store(zarr_store: FSMap, zarr_target: FSMap,
                       zarr_temp: FSMap) -> str:
    """Rechunks a zarr store that was created by the mosaic_to_zarr processes.

    This is specific to tuning output zarr store variables to the chunksizes
    given by compute_chunksize.
    """
    target_chunks = get_target_chunks(zarr_store)
    opened_zarr_store = open_consolidated(zarr_store, mode='r')
    # This is a best guess on trial and error with an 8Gi Memory container
    max_memory = '1GB'
    array_plan = rechunk(opened_zarr_store,
                         target_chunks,
                         max_memory,
                         zarr_target,
                         temp_store=zarr_temp)
    array_plan.execute()
    consolidate_metadata(zarr_target)


def get_target_chunks(zarr_store: FSMap) -> Dict:
    """Determine the chuncking strategy for the input zarr store's variables.

    Iterate through the zarr store, computing new chunksizes for all variables
    that are not coordinates or coordinate bounds. Return a dictionary of the
    variable and new chunksizes to be used in the rechunker.
    """
    zarr_groups = _groups_from_zarr(zarr_store)

    target_chunks = {}
    # open with xr for each group?
    for group in zarr_groups:
        group_dataset = xr.open_dataset(zarr_store,
                                        group=group,
                                        mode='r',
                                        engine='zarr')
        for variable, varinfo in group_dataset.data_vars.items():
            if not _bounds(variable):
                target_chunks[f'{group}/{variable}'] = compute_chunksize(
                    varinfo.shape, varinfo.dtype)
            else:
                target_chunks[f'{group}/{variable}'] = None

        for variable in group_dataset.coords.keys():
            target_chunks[f'{group}/{variable}'] = None

    return target_chunks


def _bounds(variable: str) -> bool:
    """Determines if a variable is a bounds type variable."""
    return variable.endswith(('_bnds', '_bounds'))


def _groups_from_zarr(zarr_root: str) -> List[str]:
    """Get the name of all groups in the zarr_store."""
    original_zarr = open_consolidated(zarr_root, mode='r')
    groups = ['']

    def is_group(name: str) -> None:
        """Create function to test if the item is a group or not."""
        if isinstance(original_zarr.get(name), zarrGroup):
            groups.append(name)

    original_zarr.visit(is_group)

    return groups
