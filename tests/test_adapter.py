"""
Tests the Harmony adapter, including end-to-end tests of Harmony CLI invocations
"""

import argparse
import logging
import os
import tempfile
import textwrap
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase
from unittest.mock import patch, call

from harmony.util import config as harmony_config
from netCDF4 import Dataset
from numpy import dtype
from pystac import Catalog
from zarr import DirectoryStore, open_consolidated

from harmony.message import Message
from harmony_netcdf_to_zarr.__main__ import main
from harmony_netcdf_to_zarr.adapter import NetCDFToZarrAdapter, ZarrException

from tests.util.file_creation import (ROOT_METADATA_VALUES,
                                      create_full_dataset,
                                      create_input_catalog,
                                      create_large_dataset)
from tests.util.harmony_interaction import MOCK_ENV, mock_message

logger = logging.getLogger()


class TestAdapter(TestCase):
    """ Tests the Harmony adapter """
    def setUp(self):
        self.maxdiff = None
        self.config = harmony_config(validate=False)
        self.metadata_dir = mkdtemp()
        self.temp_dir = mkdtemp()

    def tearDown(self):
        rmtree(self.metadata_dir)
        rmtree(self.temp_dir)

    @patch('harmony_netcdf_to_zarr.convert.__copy_aggregated_dimension')
    @patch('harmony_netcdf_to_zarr.adapter.make_s3fs')
    @patch('harmony_netcdf_to_zarr.convert.make_s3fs')
    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch.dict(os.environ, MOCK_ENV)
    def test_end_to_end_file_conversion(self, mock_download, mock_make_s3fs,
                                        mock_make_s3fs_adapter,
                                        mock_copy_aggregated_dimension):
        """ Full end-to-end test of the adapter from call to `main` to Harmony
            STAC catalog output, including ensuring the contents of the file
            are correct.

            Mocks S3 interactions with a local Zarr file store and download of
            granules due to `moto` and `multiprocessing` incompatibility
            issues.

            The `__copy_aggregated_dimension` function is also mocked, to show
            that the `DimensionsMapping` class created for a single granule
            request does not contain any aggregated dimension. This indicates
            that even when an input dimension (e.g., time) is irregular, the
            service will not try to create a regular grid, but instead will
            exactly output the input dimension and grid.

        """
        local_zarr = DirectoryStore(os.path.join(self.temp_dir, 'test.zarr'))
        mock_make_s3fs_adapter.return_value.get_mapper.return_value = local_zarr
        mock_make_s3fs.return_value.get_mapper.return_value = local_zarr

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

        # Assert that no aggregation was attempted in the output Zarr:
        mock_copy_aggregated_dimension.assert_not_called()

        # Assertions to ensure STAC output contains correct items, and the
        # new output item has the correct temporal and spatial extents
        output_catalog = Catalog.from_file(os.path.join(self.metadata_dir,
                                                        'catalog.json'))
        # There should be one item in the output catalog: the output Zarr store
        output_items = list(output_catalog.get_items())
        self.assertEqual(len(output_items), 1)

        # Read the input STAC catalog for spatial and temporal comparisons:
        input_catalog = Catalog.from_file(stac_catalog_path)
        input_item = list(input_catalog.get_items())[0]

        self.assertListEqual(output_items[0].bbox, input_item.bbox)
        self.assertEqual(output_items[0].common_metadata.start_datetime,
                         input_item.common_metadata.start_datetime)
        self.assertEqual(output_items[0].common_metadata.end_datetime,
                         input_item.common_metadata.end_datetime)

        out = open_consolidated(local_zarr)

        # -- Hierarchical Structure Assertions --
        contents = textwrap.dedent("""
            /
             ├── data
             │   ├── horizontal
             │   │   ├── east (1, 3, 3) uint8
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
        self.assertEqual(out['data/horizontal/west'][0, 0, 0], 16)
        self.assertEqual(out['data/horizontal/west'][0, 2, 2], 0)
        # East variable has a scale factor; raw values should still be copied:
        self.assertEqual(out['data/horizontal/east'][0, 2, 2], 8)
        self.assertEqual(out['data/horizontal/east'][0, 0, 0], 0)

        # 'east' attributes scale_factor removed
        self.assertFalse(hasattr(out['data/horizontal/east'], 'scale_factor'))

        # 'east' attributes present, exactly matching input attributes:
        self.assertEqual(out['data/horizontal/east'].attrs['valid_range'], [0.0, 25.0])
        self.assertEqual(out['data/horizontal/east'].attrs['valid_min'], 0.0)
        self.assertEqual(out['data/horizontal/east'].attrs['valid_max'], 25.0)
        self.assertEqual(out['data/horizontal/east'].attrs['_FillValue'], 127.0)
        self.assertEqual(out['data/horizontal/east'].attrs['scale_factor'], 2.0)
        self.assertFalse(hasattr(out['data/horizontal/east'], 'missing_value'))

        # 2D Nested Float Arrays
        self.assertEqual(out['location/lat'][0, 1], 5.5)
        self.assertEqual(out['location/lon'][0, 1], -5.5)

        # 1D Root-Level Float Array sharing its name with a dimension
        self.assertEqual(out['time'][0], 166536)

    @patch('harmony_netcdf_to_zarr.adapter.make_s3fs')
    @patch('harmony_netcdf_to_zarr.convert.make_s3fs')
    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch.dict(os.environ, MOCK_ENV)
    def test_end_to_end_large_file_conversion(self, mock_download,
                                              mock_make_s3fs,
                                              mock_make_s3fs_adapter):
        """ Full end-to-end test of the adapter to make sure rechunk is
            working. Mocks S3 interactions using @mock_s3.

            Mocks download of granules due to `moto` and `multiprocessing`
            incompatibility issues.

        """
        local_zarr = DirectoryStore(os.path.join(self.temp_dir, 'test.zarr'))
        mock_make_s3fs_adapter.return_value.get_mapper.return_value = local_zarr
        mock_make_s3fs.return_value.get_mapper.return_value = local_zarr

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

        # There should be one item in the output catalog: the output Zarr store
        output_items = list(output_catalog.get_items())
        self.assertEqual(len(output_items), 1)

        out = open_consolidated(local_zarr)

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

    @patch('harmony_netcdf_to_zarr.convert.compute_chunksize')
    @patch('harmony_netcdf_to_zarr.adapter.make_s3fs')
    @patch('harmony_netcdf_to_zarr.convert.make_s3fs')
    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch.dict(os.environ, MOCK_ENV)
    def test_end_to_end_mosaic(self, mock_download, mock_make_s3fs,
                               mock_make_s3fs_adapter, mock_compute_chunksize):
        """ Full end-to-end test of the adapter from call to `main` to Harmony
            STAC catalog output for multiple input granules, including ensuring
            the contents of the file are correct. This should produce a single
            Zarr output.

            Mocks S3 interactions with a local Zarr file store and download of
            granules due to `moto` and `multiprocessing` incompatibility
            issues.

        """
        local_zarr = DirectoryStore(os.path.join(self.temp_dir, 'test.zarr'))
        mock_make_s3fs_adapter.return_value.get_mapper.return_value = local_zarr
        mock_make_s3fs.return_value.get_mapper.return_value = local_zarr

        def chunksize_side_effect(input_array_size, _):
            """ Set compute_chunksize mock to return the input array size """
            return list(input_array_size)

        mock_compute_chunksize.side_effect = chunksize_side_effect

        # Create mock data. Science variable and time for second NetCDF-4 must
        # be different to first to allow mosaic testing.
        first_file = create_full_dataset()
        second_file = create_full_dataset()

        with Dataset(second_file, 'r+') as dataset:
            dataset['time'][:] += 1800
            dataset['/data/vertical/north'][:] += 1
            dataset['/data/vertical/south'][:] += 1

        stac_catalog_path = create_input_catalog([first_file, second_file])
        mock_download.return_value = [first_file, second_file]

        try:
            message = mock_message()
            main(['harmony_netcdf_to_zarr', '--harmony-action', 'invoke',
                  '--harmony-input', message, '--harmony-sources',
                  stac_catalog_path, '--harmony-metadata-dir',
                  self.metadata_dir],
                 config=self.config)
        finally:
            os.remove(first_file)
            os.remove(second_file)

        # Assertions to ensure STAC output contains correct items, and the
        # new output item has the correct temporal and spatial extents
        output_catalog = Catalog.from_file(os.path.join(self.metadata_dir,
                                                        'catalog.json'))

        # Read input catalog for bounding box and temporal comparisons:
        input_catalog = Catalog.from_file(stac_catalog_path)
        input_item = list(input_catalog.get_items())[0]

        # There should be one item in the output catalog: the output Zarr store
        output_items = list(output_catalog.get_items())
        self.assertEqual(len(output_items), 1)

        self.assertListEqual(output_items[0].bbox, input_item.bbox)
        self.assertEqual(output_items[0].common_metadata.start_datetime,
                         input_item.common_metadata.start_datetime)
        self.assertEqual(output_items[0].common_metadata.end_datetime,
                         input_item.common_metadata.end_datetime)

        out = open_consolidated(local_zarr)

        # -- Hierarchical Structure Assertions --
        contents = textwrap.dedent("""
            /
             ├── data
             │   ├── horizontal
             │   │   ├── east (2, 3, 3) uint8
             │   │   └── west (2, 3, 3) uint8
             │   └── vertical
             │       ├── north (2, 3, 3) uint8
             │       └── south (2, 3, 3) uint8
             ├── location
             │   ├── lat (3, 3) float32
             │   └── lon (3, 3) float32
             └── time (2,) int32
            """).strip()
        self.assertEqual(str(out.tree()), contents)

        # -- behavior assertion
        mock_compute_chunksize.assert_has_calls([call((1, ), dtype('int32')),
                                                 call((3, 3), dtype('float32')),
                                                 call((3, 3), dtype('float32')),
                                                 call((1, 3, 3), dtype('uint8')),
                                                 call((1, 3, 3), dtype('uint8')),
                                                 call((1, 3, 3), dtype('uint8')),
                                                 call((1, 3, 3), dtype('uint8'))],
                                                any_order=True)
        single_files_variable_count = 7
        self.assertLessEqual(mock_compute_chunksize.call_count, single_files_variable_count)

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
        self.assertEqual(out['data/horizontal/west'][0, 0, 0], 16)
        self.assertEqual(out['data/horizontal/west'][0, 2, 2], 0)
        self.assertEqual(out['data/vertical/north'][1, 0, 2], 17)
        self.assertEqual(out['data/vertical/north'][1, 2, 0], 1)
        self.assertEqual(out['data/vertical/south'][1, 2, 0], 17)
        self.assertEqual(out['data/vertical/south'][1, 0, 2], 1)
        # East variable has scale factor; raw values should still be copied:
        self.assertEqual(out['data/horizontal/east'][0, 2, 2], 8)
        self.assertEqual(out['data/horizontal/east'][0, 0, 0], 0)

        # 'east' attributes present, exactly matching input attributes:
        self.assertEqual(out['data/horizontal/east'].attrs['valid_range'], [0.0, 25.0])
        self.assertEqual(out['data/horizontal/east'].attrs['valid_min'], 0.0)
        self.assertEqual(out['data/horizontal/east'].attrs['valid_max'], 25.0)
        self.assertEqual(out['data/horizontal/east'].attrs['_FillValue'], 127.0)
        self.assertEqual(out['data/horizontal/east'].attrs['scale_factor'], 2.0)
        self.assertFalse(hasattr(out['data/horizontal/east'], 'missing_value'))

        # 2D Nested Float Arrays
        self.assertEqual(out['location/lat'][0, 1], 5.5)
        self.assertEqual(out['location/lon'][0, 1], -5.5)

        # 1D Root-Level Float Array sharing its name with a dimension
        self.assertEqual(out['time'][0], 166536)
        self.assertEqual(out['time'][1], 168336)

    @patch.object(argparse.ArgumentParser, 'error', return_value=None)
    def test_does_not_accept_non_harmony_clis(self, argparse_error):
        """
        Tests that when a non-Harmony argument gets passed, the CLI returns an error
        """
        main(['harmony_netcdf_to_zarr', '--discord'])
        argparse_error.assert_called_with('Only --harmony CLIs are supported')

    @patch.dict(os.environ, dict(USE_LOCALSTACK='true', LOCALSTACK_HOST='fake-host'))
    def test_localstack_client(self):
        """ Tests that when USE_LOCALSTACK and LOCALSTACK_HOST are supplied the
            adapter uses localstack

        """
        adapter = NetCDFToZarrAdapter(Message(mock_message()))
        self.assertEqual(adapter.s3.client_kwargs['endpoint_url'],
                         'http://fake-host:4572')

    @patch.dict(os.environ, MOCK_ENV)
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
            context_manager.exception.message.startswith(
                'Could not create Zarr output:'
            )
        )

        os.remove(filename)
