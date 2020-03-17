"""
=========
__main__.py
=========

Runs the harmony_netcdf_to_zarr CLI
"""

import sys
import argparse
import logging
import harmony

from .adapter import NetCDFToZarrAdapter as HarmonyAdapter

def main(argv):
    """
    Parses command line arguments and invokes the appropriate method to respond to them

    Returns
    -------
    None
    """
    parser = argparse.ArgumentParser(
        prog='harmony-netcdf-to-zarr', description='Run the NetCDF4 to Zarr')
    harmony.setup_cli(parser)
    args = parser.parse_args(argv[1:])
    if (harmony.is_harmony_cli(args)):
        harmony.run_cli(parser, args, HarmonyAdapter)
    else:
        parser.error("Only --harmony CLIs are supported")

if __name__ == "__main__":
    main(sys.argv)
