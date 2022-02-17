"""
Tests the Harmony adapter, including end-to-end tests of Harmony CLI invocations
"""

import argparse
import logging
import os
import tempfile
import textwrap
import unittest
from shutil import rmtree
from tempfile import mkdtemp
from unittest.mock import patch

import boto3
import s3fs
import zarr
from harmony.util import config as harmony_config
from moto import mock_s3
from multiprocessing.popen_fork import Popen as mp_Popen
from multiprocessing import util as mp_util
from pystac import Catalog

from harmony.message import Message
from harmony_netcdf_to_zarr.__main__ import main
from harmony_netcdf_to_zarr.adapter import NetCDFToZarrAdapter, ZarrException

from .util.file_creation import (ROOT_METADATA_VALUES, create_full_dataset,
                                 create_input_catalog, create_large_dataset)
from .util.harmony_interaction import MOCK_ENV, mock_message

logger = logging.getLogger()


def mock_mp_fork_popen_launch(class_object, process_obj):
    """ `moto` is used to mock S3, however, `moto` does not work with
        multiprocessing, because it holds objects in memory, and then the
        objects are gone when the child processes quit. This method mocks
        `multiprocessing.popen_fork.Popen._launch` to do the work in the
        parent process.

    Parameters
    ----------
    class_object: object
        class object for multiprocessing.popen_fork.Popen
    process_obj: object
        task object to be processed by multiprocessing.popen_fork.Popen._launch
    """

    def attempt_to_close(*fds):
        """ Mimic `multiprocessing.util.close_fds`, but wrap the `os.close`
            statement with a test for `None`, to avoid issues with tests.

        """
        for fd in fds:
            if fd is not None:
                os.close(fd)

    code = 1
    parent_r, child_w = os.pipe()
    child_r, parent_w = os.pipe()
    class_object.pid = os.fork()
    if class_object.pid == 0:
        code = 0
        os._exit(code)
    else:
        code = process_obj._bootstrap()
        os.close(child_w)
        os.close(child_r)
        class_object.finalizer = mp_util.Finalize(class_object, attempt_to_close,
                                                  (parent_r, parent_w,))
        class_object.sentinel = parent_r


class TestAdapter(unittest.TestCase):
    """ Tests the Harmony adapter """
    def setUp(self):
        self.maxdiff = None
        self.config = harmony_config(validate=False)
        self.metadata_dir = mkdtemp()

    def tearDown(self):
        rmtree(self.metadata_dir)

    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch.dict(os.environ, MOCK_ENV)
    @patch.object(mp_Popen, '_launch', new=mock_mp_fork_popen_launch)
    @mock_s3
    def test_end_to_end_file_conversion(self, mock_download):
        """ Full end-to-end test of the adapter from call to `main` to Harmony
            STAC catalog output, including ensuring the contents of the file
            are correct.

            Mocks S3 interactions using @mock_s3.
            Also mocks download of granules due to `moto` and `multiprocessing`
            incompatibility issues.

        """
        conn = boto3.resource('s3')
        conn.create_bucket(
            Bucket='example-bucket',
            CreateBucketConfiguration={'LocationConstraint': os.environ['AWS_DEFAULT_REGION']})

        netcdf_file = create_full_dataset()
        stac_catalog_path = create_input_catalog([netcdf_file])
        mock_download.return_value = [netcdf_file]

        try:
            message = mock_message()
            main(['harmony_netcdf_to_zarr', '--harmony-action', 'invoke',
                  '--harmony-input', message, '--harmony-sources',
                  stac_catalog_path, '--harmony-metadata-dir',
                  self.metadata_dir],
                 config=self.config)
        finally:
            os.remove(netcdf_file)

        # Assertions to ensure STAC output contains correct items, and the
        # new output item has the correct temporal and spatial extents
        output_catalog = Catalog.from_file(os.path.join(self.metadata_dir,
                                                        'catalog.json'))
        # There should be two items in the output catalog - the input and
        # the output Zarr store
        output_items = list(output_catalog.get_items())
        self.assertEqual(len(output_items), 2)

        # The Zarr STAC item will not start with 'id', unlike the input items.
        # The first input Zarr item will have 'id0':
        input_item = output_catalog.get_item('id0')
        output_item = next(item for item in output_items
                           if not item.id.startswith('id'))

        self.assertListEqual(output_item.bbox, input_item.bbox)
        self.assertEqual(output_item.common_metadata.start_datetime,
                         input_item.common_metadata.start_datetime)
        self.assertEqual(output_item.common_metadata.end_datetime,
                         input_item.common_metadata.end_datetime)

        # Open the Zarr file using the URL from the output STAC item:
        zarr_location = output_item.assets['data'].href
        store = s3fs.S3FileSystem().get_mapper(root=zarr_location, check=False)
        out = zarr.open_consolidated(store)

        # -- Hierarchical Structure Assertions --
        contents = textwrap.dedent("""
            /
             ├── data
             │   ├── horizontal
             │   │   ├── east (1, 3, 3) int64
             │   │   └── west (1, 3, 3) uint8
             │   └── vertical
             │       ├── north (1, 3, 3) uint8
             │       └── south (1, 3, 3) uint8
             ├── location
             │   ├── lat (3, 3) float32
             │   └── lon (3, 3) float32
             └── time (1,) int32
            """).strip()
        self.assertEqual(str(out.tree()), contents)

        # -- Metadata Assertions --
        # Root level values
        self.assertEqual(dict(out.attrs), ROOT_METADATA_VALUES)

        # Group metadata
        self.assertEqual(out['data'].attrs['description'], 'Group to hold the data')

        # Variable metadata
        var = out['data/vertical/north']
        self.assertEqual(var.attrs['coordinates'], 'lon lat')

        # -- Data Assertions --
        # Nested Byte Arrays
        self.assertEqual(out['data/vertical/north'][0, 0, 2], 16)
        self.assertEqual(out['data/vertical/north'][0, 2, 0], 0)
        self.assertEqual(out['data/vertical/south'][0, 2, 0], 16)
        self.assertEqual(out['data/vertical/south'][0, 0, 2], 0)
        self.assertEqual(out['data/horizontal/east'][0, 2, 2], 16)  # scale_factor = 2
        self.assertEqual(out['data/horizontal/east'][0, 0, 0], 0)
        self.assertEqual(out['data/horizontal/west'][0, 0, 0], 16)
        self.assertEqual(out['data/horizontal/west'][0, 2, 2], 0)

        # 'east' attributes scale_factor removed
        self.assertFalse(hasattr(out['data/horizontal/east'], 'scale_factor'))

        # 'east' attributes present and scaled
        self.assertEqual(out['data/horizontal/east'].attrs['valid_range'], [0.0, 50.0])
        self.assertEqual(out['data/horizontal/east'].attrs['valid_min'], 0.0)
        self.assertEqual(out['data/horizontal/east'].attrs['valid_max'], 50.0)
        self.assertEqual(out['data/horizontal/east'].attrs['_FillValue'], 254.0)
        self.assertFalse(hasattr(out['data/horizontal/east'], 'missing_value'))

        # 2D Nested Float Arrays
        self.assertEqual(out['location/lat'][0, 1], 5.5)
        self.assertEqual(out['location/lon'][0, 1], -5.5)

        # 1D Root-Level Float Array sharing its name with a dimension
        self.assertEqual(out['time'][0], 166536)

    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch.dict(os.environ, MOCK_ENV)
    @patch.object(mp_Popen, '_launch', new=mock_mp_fork_popen_launch)
    @mock_s3
    def test_end_to_end_large_file_conversion(self, mock_download):
        """ Full end-to-end test of the adapter to make sure rechunk is
            working. Mocks S3 interactions using @mock_s3.

            Currently only uses a single NetCDF-4 input, as concatenation will
            be supported only starting with DAS-1379.

            Mocks download of granules due to `moto` and `multiprocessing`
            incompatibility issues.

        """
        conn = boto3.resource('s3')
        conn.create_bucket(
            Bucket='example-bucket',
            CreateBucketConfiguration={'LocationConstraint': os.environ['AWS_DEFAULT_REGION']})

        netcdf_file = create_large_dataset()
        stac_catalog_path = create_input_catalog([netcdf_file])
        mock_download.return_value = [netcdf_file]

        try:
            message = mock_message()
            main(['harmony_netcdf_to_zarr', '--harmony-action', 'invoke',
                  '--harmony-input', message, '--harmony-sources',
                  stac_catalog_path, '--harmony-metadata-dir', self.metadata_dir],
                 config=self.config)
        finally:
            os.remove(netcdf_file)

        output_catalog = Catalog.from_file(os.path.join(self.metadata_dir,
                                                        'catalog.json'))

        # There should be two items in the output catalog - the input and
        # the output Zarr store
        output_items = list(output_catalog.get_items())
        self.assertEqual(len(output_items), 2)

        # The Zarr STAC item will not start with 'id', unlike the input items.
        # The first input Zarr item will have 'id0':
        output_item = next(item for item in output_items
                           if not item.id.startswith('id'))

        # Open the Zarr file using the URL from the output STAC item:
        zarr_location = output_item.assets['data'].href
        store = s3fs.S3FileSystem().get_mapper(root=zarr_location, check=False)
        out = zarr.open_consolidated(store)

        # -- Hierarchical Structure Assertions --
        contents = textwrap.dedent("""
            /
             ├── data
             │   └── var (10000,) int32
             └── dummy_dim (10000,) int32
            """).strip()
        self.assertEqual(str(out.tree()), contents)

        # -- Data Assertions --
        self.assertEqual(out['data/var'].chunks, (10000,))

    @patch.object(argparse.ArgumentParser, 'error', return_value=None)
    def test_does_not_accept_non_harmony_clis(self, argparse_error):
        """
        Tests that when a non-Harmony argument gets passed, the CLI returns an error
        """
        main(['harmony_netcdf_to_zarr', '--discord'])
        argparse_error.assert_called_with('Only --harmony CLIs are supported')

    @patch.dict(os.environ, dict(USE_LOCALSTACK='true', LOCALSTACK_HOST='fake-host'))
    def test_localstack_client(self):
        """
        Tests that when USE_LOCALSTACK and LOCALSTACK_HOST are supplied the adapter uses localstack
        """
        adapter = NetCDFToZarrAdapter(Message(mock_message()))
        self.assertEqual(adapter.s3.client_kwargs['endpoint_url'], 'http://fake-host:4572')

    @patch.dict(os.environ, MOCK_ENV)
    @mock_s3
    def test_conversion_failure(self):
        """ Tests that when file conversion fails, e.g. due to a corrupted
            input file, Harmony catches an exception and them the exception
            gets rethrown as a `ZarrException`.

        """
        filename = tempfile.mkstemp(suffix='.nc4')[1]
        stac_catalog = create_input_catalog([filename])

        message = mock_message()

        with self.assertRaises(ZarrException) as context_manager:
            main(['harmony_netcdf_to_zarr', '--harmony-action', 'invoke',
                  '--harmony-input', message, '--harmony-sources',
                  stac_catalog, '--harmony-metadata-dir', self.metadata_dir],
                 config=self.config)

            self.assertTrue(
                str(context_manager.exception).startswith(
                    'Could not create Zarr output:'
                )
            )

        os.remove(filename)
