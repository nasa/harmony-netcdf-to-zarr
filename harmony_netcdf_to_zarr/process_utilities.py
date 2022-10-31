from multiprocessing import Process
from multiprocessing.managers import Namespace
from time import sleep
from typing import List


def monitor_processes(processes: List[Process], shared_namespace: Namespace, error_notice: str) -> None:
    """Monitor multiprocess processes for errors.

    Monitor the running processes to see if any are killed outside
    of the running code, process checks
    """
    alive_array = []
    exit_codes = []
    for _ in range(len(processes)):
        alive_array.append(True)
        exit_codes.append(None)

    while any(aa is True for aa in alive_array):
        sleep(1)
        for i, process in enumerate(processes):
            exit_codes[i] = process.exitcode
            alive_array[i] = process.is_alive()
            if (process.exitcode not in [None, 0]):
                shared_namespace.process_error = 'process error occurred'

    for output_process in processes:
        output_process.join()
        output_process.close()

    if not all(code == 0 for code in exit_codes):
        raise RuntimeError(f'{error_notice}: processes exit codes: {exit_codes}')
