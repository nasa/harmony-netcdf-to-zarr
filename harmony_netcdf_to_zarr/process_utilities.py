from multiprocessing import Process
from multiprocessing.managers import Namespace
from time import sleep
from typing import List


def monitor_processes(processes: List[Process], shared_namespace: Namespace, error_notice: str) -> None:
    """Monitor multiprocess processes for errors.

    Run and monitor multiprocessing processes ensure successful exits.
    """
    for process in processes:
        process.start()

    while any(process.is_alive() for process in processes):
        sleep(.5)
        if any(process.exitcode not in [None, 0] for process in processes):
            shared_namespace.process_error = 'process error occurred'

    exit_codes = []
    for process in processes:
        process.join()
        exit_codes.append(process.exitcode)
        process.close()

    if hasattr(shared_namespace, 'exception'):
        raise RuntimeError(f'{error_notice}: {shared_namespace.exception}')

    if hasattr(shared_namespace, 'process_error'):
        raise RuntimeError(f'{error_notice}: processes exit codes: {exit_codes}')
