""" A utility module for performing multiple downloads simultaneously.
    This is largely derived from the PO.DAAC Concise service:

    https://github.com/podaac/concise/blob/develop/podaac/merger/harmony/download_worker.py

"""
from copy import deepcopy
from logging import Logger
from multiprocessing import Manager, Process, Queue
from multiprocessing.managers import Namespace
from os import cpu_count
from queue import Empty as QueueEmpty
from typing import List

from harmony.util import Config, download

from harmony_netcdf_to_zarr.process_utilities import monitor_processes


def download_granules(netcdf_urls: List[str], destination_directory: str,
                      access_token: str, harmony_config: Config,
                      logger: Logger, process_count: int = None) -> List[str]:
    """ A method which scales concurrent downloads to the number of available
        CPU cores. For further explanation, see documentation on "multi-track
        drifting"

        The number of processes is limited to the minimum of:

        * The number of available CPUs.
        * The number of requested NetCDF-4 files.
        * An user-supplied number of processes (if specified).

    """
    logger.info('Beginning granule downloads.')

    if process_count is None:
        process_count = min(cpu_count(), len(netcdf_urls))
    else:
        process_count = min(process_count, cpu_count(), netcdf_urls)

    with Manager() as manager:
        download_queue = manager.Queue(len(netcdf_urls))
        shared_namespace = manager.Namespace()
        local_paths = manager.list()

        for netcdf_url in netcdf_urls:
            download_queue.put(netcdf_url)

        # Spawn a worker process for each CPU being used
        processes = [Process(target=_download_worker,
                             args=(download_queue, shared_namespace,
                                   local_paths, destination_directory,
                                   access_token, harmony_config, logger))
                     for _ in range(process_count)]

        monitor_processes(processes, shared_namespace, error_notice='Download failed')

        # Copy paths so they persist outside of the Manager context.
        download_paths = deepcopy(local_paths)

    logger.info('Finished downloading granules')

    return download_paths


def _download_worker(download_queue: Queue, shared_namespace: Namespace,
                     local_paths: List, destination_dir: str,
                     access_token: str, harmony_config: Config, logger: Logger):
    """ A method to be executed in a separate process. This will check for
        items in the queue, which correspond to URLs for NetCDF-4 files to
        download. If there is at least one URL left for download, then it is
        retrieved from the queue and the `harmony-py.util.download` function
        is used to retrieve the granule. Otherwise, the process is ended. All
        local paths of downloaded granules are added to a list that is
        administered by the process manager instance.

    """
    while (not hasattr(shared_namespace, 'exception')
           and not hasattr(shared_namespace, 'process_error')
           and not download_queue.empty()):
        try:
            netcdf_url = download_queue.get_nowait()
        except QueueEmpty:
            break

        try:
            local_path = download(netcdf_url, destination_dir, logger=logger,
                                  access_token=access_token,
                                  cfg=harmony_config)

            local_paths.append(local_path)
        except Exception as exception:
            # If there was an issue, save a string message from the raised
            # exception. This will cause other processes to stop downloads.
            shared_namespace.exception = str(exception)
            raise exception
