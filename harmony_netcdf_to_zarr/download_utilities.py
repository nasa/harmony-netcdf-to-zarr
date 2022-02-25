""" A utility module for performing multiple downloads simultaneously.
    This is largely derived from the PO.DAAC Concise service:

    https://github.com/podaac/concise/blob/develop/podaac/merger/harmony/download_worker.py

"""
from copy import deepcopy
from logging import Logger
from multiprocessing import Manager, Process, Queue
from os import cpu_count
from queue import Empty as QueueEmpty
from typing import List

from harmony.util import Config, download


def download_granules(netcdf_urls: List[str], destination_directory: str,
                      access_token: str, harmony_config: Config,
                      logger: Logger, process_count: int = None) -> List[str]:
    """ A method which scales concurrent downloads to the number of available
        CPU cores. For further explanation, see documentation on "multi-track
        drifting"

    """
    logger.info('Beginning granule downloads.')

    if process_count is None:
        process_count = cpu_count()
    else:
        process_count = min(process_count, cpu_count())

    with Manager() as manager:
        download_queue = manager.Queue(len(netcdf_urls))
        local_paths = manager.list()

        for netcdf_url in netcdf_urls:
            download_queue.put(netcdf_url)

        # Spawn a worker process for each CPU being used
        processes = [Process(target=_download_worker,
                             args=(download_queue, local_paths,
                                   destination_directory, access_token,
                                   harmony_config, logger))
                     for _ in range(process_count)]

        for download_process in processes:
            download_process.start()

        # Ensure worker processes exit successfully
        for download_process in processes:
            download_process.join()
            if download_process.exitcode != 0:
                raise RuntimeError('Download failed - exit code: '
                                   f'{download_process.exitcode}')

            download_process.close()

        # Copy paths so they persist outside of the Manager context.
        download_paths = deepcopy(local_paths)

    logger.info('Finished downloading granules')

    return download_paths


def _download_worker(download_queue: Queue, local_paths: List,
                     destination_dir: str, access_token: str,
                     harmony_config: Config, logger: Logger):
    """ A method to be executed in a separate process. This will check for
        items in the queue, which correspond to URLs for NetCDF-4 files to
        download. If there is at least one URL left for download, then it is
        retrieved from the queue and the `harmony-py.util.download` function
        is used to retrieve the granule. Otherwise, the process is ended. All
        local paths of downloaded granules are added to a list that is
        administered by the process manager instance.

    """
    while not download_queue.empty():
        try:
            netcdf_url = download_queue.get_nowait()
        except QueueEmpty:
            break

        local_path = download(netcdf_url, destination_dir, logger=logger,
                              access_token=access_token, cfg=harmony_config)

        local_paths.append(local_path)
