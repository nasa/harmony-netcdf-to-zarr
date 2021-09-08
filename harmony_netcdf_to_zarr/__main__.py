"""
=========
__main__.py
=========

Runs the harmony_netcdf_to_zarr CLI
"""

import sys

if sys.version_info[0] != 3 or sys.version_info[1] < 6:
    raise Exception('You must use Python 3.6 or later')

import argparse

import harmony

from .adapter import NetCDFToZarrAdapter as HarmonyAdapter


def main(argv, **kwargs):
    """
    Parses command line arguments and invokes the appropriate method to respond to them

    Returns
    -------
    None
    """
    # TODO - Update this when working HARMONY-639
    # DO NOT REMOVE THE FOLLOWING LINE - NEEDED AS WORKAROUND TO ARGO CHAINING ISSUE
    print('MAIN STARTED')

    config = None
    # Optional: harmony.util.Config is injectable for tests
    if 'config' in kwargs:
        config = kwargs.get('config')

    parser = argparse.ArgumentParser(
        prog='harmony-netcdf-to-zarr', description='Run the NetCDF4 to Zarr')
    harmony.setup_cli(parser)
    args = parser.parse_args(argv[1:])
    if (harmony.is_harmony_cli(args)):
        harmony.run_cli(parser, args, HarmonyAdapter, cfg=config)
    else:
        parser.error("Only --harmony CLIs are supported")


if __name__ == "__main__":
    main(sys.argv)
