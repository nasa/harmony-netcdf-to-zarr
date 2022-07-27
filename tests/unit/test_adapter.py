""" Unit tests for the `harmony_netcdf_to_zarr.adapter` module. """
from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

from harmony.message import Message
from harmony.util import bbox_to_geometry, config
from pystac import Asset, Catalog, Item, Link
from s3fs import S3FileSystem

from harmony_netcdf_to_zarr.adapter import NetCDFToZarrAdapter, ZarrException


class TestNetCDFToZarrAdapter(TestCase):
    """ Unit tests for methods in the `NetCDFToZarrAdapter` class. These tests
        try to isolate each method from external calls.

    """
    @classmethod
    def setUpClass(cls):
        """ Set test fixtures that can be defined once for all tests. """
        cls.bbox = [-20, -10, 10, 20]
        cls.bbox_geometry = bbox_to_geometry(cls.bbox)
        cls.collection = 'C1234567890-PROV'
        cls.granule_datetime = datetime(2000, 1, 1)
        cls.harmony_config = config(validate=False)
        cls.base_message_content = {'accessToken': 'supersecret',
                                    'callback': 'callbackURL',
                                    'sources': [{'collection': cls.collection}],
                                    'stagingLocation': 'stagingURL',
                                    'user': 'ascientist'}

    def setUp(self):
        """ Set test fixtures that need to be defined once per test. """

    def tearDown(self):
        """ Remove Dataset test fixture between tests. The `DimensionsMapping`
            class will close each `Dataset` it parses, so must check if the
            `Dataset` is still open before trying to close it.

        """

    @patch('harmony_netcdf_to_zarr.adapter.make_localstack_s3fs')
    @patch.object(NetCDFToZarrAdapter, 'process_items_many_to_one')
    def test_invoke_single_input(self, mock_process_items,
                                 mock_make_localstack):
        """ Ensure the invoke method works when a single input granule is
            specified via a STAC catalog, and the Harmony message requests the
            expected output format.

        """
        mock_output = Catalog('output', 'process_items_many_to_one return')
        mock_process_items.return_value = mock_output

        granule_url = 'https://example.com/amazing_file.nc4'
        stac_catalog = Catalog('single_input', 'description of test catalog')
        stac_item = Item('id1', self.bbox_geometry, self.bbox,
                         self.granule_datetime, {})
        stac_item.add_asset('data', Asset(granule_url, roles=['data'],
                                          media_type='application/x-netcdf4'))
        stac_catalog.add_item(stac_item)

        message_content = self.base_message_content.copy()
        message_content['format'] = {'mime': 'application/x-zarr'}
        harmony_message = Message(message_content)

        harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                              catalog=stac_catalog,
                                              config=self.harmony_config)

        output_message, output_catalog = harmony_adapter.invoke()
        self.assertEqual(output_message, harmony_message)
        self.assertEqual(output_catalog, mock_output)

    @patch('harmony_netcdf_to_zarr.adapter.make_localstack_s3fs')
    @patch.object(NetCDFToZarrAdapter, 'process_items_many_to_one')
    @patch('harmony.adapter.read_file')
    def test_invoke_multiple_inputs(self, test_patch, mock_process_items,
                                    mock_make_localstack):
        """ Ensure the invoke method works when multiple input granules are
            specified via a STAC catalog and the Harmony message requests the
            expected output format.

        """
        mock_output = Catalog('output', 'process_items_many_to_one return')
        mock_process_items.return_value = mock_output

        granule_url = 'https://example.com/amazing_file.nc4'
        stac_catalog0 = Catalog('multiple_input', 'description of test catalog 0')
        stac_catalog1 = Catalog('multiple_input', 'description of test catalog 1')
        stac_catalog0.add_link(Link('next', 'catalog1.json'))
        stac_catalog1.add_link(Link('prev', 'catalog0.json'))
        stac_item_one = Item('id1', self.bbox_geometry, self.bbox,
                             self.granule_datetime, {})
        stac_item_two = Item('id2', self.bbox_geometry, self.bbox,
                             self.granule_datetime, {})
        for stac_item in [stac_item_one, stac_item_two]:
            stac_item.add_asset(
                'data', Asset(granule_url, roles=['data'],
                              media_type='application/x-netcdf4')
            )
        stac_catalog0.add_item(stac_item_one)
        stac_catalog1.add_item(stac_item_two)
        test_patch.return_value = stac_catalog1

        message_content = self.base_message_content.copy()
        message_content['format'] = {'mime': 'application/x-zarr'}
        harmony_message = Message(message_content)

        harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                              catalog=stac_catalog0,
                                              config=self.harmony_config)

        output_message, output_catalog = harmony_adapter.invoke()
        self.assertEqual(output_message, harmony_message)
        self.assertEqual(output_catalog, mock_output)

    @patch('harmony_netcdf_to_zarr.adapter.make_localstack_s3fs')
    @patch.object(NetCDFToZarrAdapter, 'process_items_many_to_one')
    def test_invoke_failures_output_format(self, mock_process_items,
                                           mock_make_localstack):
        """ Ensure an exception is raised when a request does not specify an
            output format, or if the output format is not a MIME type for Zarr.

        """
        mock_output = Catalog('output', 'process_items_many_to_one return')
        mock_process_items.return_value = mock_output

        granule_url = 'https://example.com/amazing_file.nc4'
        stac_catalog = Catalog('single_input', 'description of test catalog')
        stac_item = Item('id1', self.bbox_geometry, self.bbox,
                         self.granule_datetime, {})
        stac_item.add_asset(
            'data', Asset(granule_url, roles=['data'],
                          media_type='application/x-netcdf4')
        )
        stac_catalog.add_item(stac_item)

        with self.subTest('Incompatible output format'):
            message_content = self.base_message_content.copy()
            message_content['format'] = {'mime': 'other/mimetype'}
            harmony_message = Message(message_content)
            harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                                  catalog=stac_catalog,
                                                  config=self.harmony_config)

            with self.assertRaises(ZarrException) as context_manager:
                harmony_adapter.invoke()

            self.assertEqual(
                context_manager.exception.message,
                'Request failed due to an incorrect service workflow'
            )

        with self.subTest('No mime attribute in message.format'):
            message_content = self.base_message_content.copy()
            message_content['format'] = {}
            harmony_message = Message(message_content)
            harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                                  catalog=stac_catalog,
                                                  config=self.harmony_config)

            with self.assertRaises(ZarrException) as context_manager:
                harmony_adapter.invoke()

            self.assertEqual(
                context_manager.exception.message,
                'Request failed due to an incorrect service workflow'
            )

        with self.subTest('No format attribute in message'):
            message_content = self.base_message_content.copy()
            harmony_message = Message(message_content)
            harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                                  catalog=stac_catalog,
                                                  config=self.harmony_config)

            with self.assertRaises(ZarrException) as context_manager:
                harmony_adapter.invoke()

            self.assertEqual(
                context_manager.exception.message,
                'Request failed due to an incorrect service workflow'
            )

    @patch('harmony_netcdf_to_zarr.adapter.make_localstack_s3fs')
    @patch.object(NetCDFToZarrAdapter, 'process_items_many_to_one')
    def test_invoke_failure_no_catalog(self, mock_process_items,
                                       mock_make_localstack):
        """ Ensure an exception is raised when the adapter is invoked without a
            STAC catalog.

        """
        message_content = self.base_message_content.copy()
        message_content['format'] = {'mime': 'application/x-zarr'}
        harmony_message = Message(message_content)
        harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                              config=self.harmony_config)

        with self.assertRaises(ZarrException) as context_manager:
            harmony_adapter.invoke()

        self.assertEqual(
            context_manager.exception.message,
            'Invoking NetCDF-to-Zarr without STAC catalog is not supported.'
        )

    @patch('harmony_netcdf_to_zarr.adapter.get_output_catalog')
    @patch('harmony_netcdf_to_zarr.adapter.netcdf_to_zarr')
    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch('harmony_netcdf_to_zarr.adapter.make_localstack_s3fs')
    def process_items_single_item(self, mock_make_localstack, mock_download,
                                  mock_netcdf_to_zarr, mock_get_catalog):
        """ Ensure a single input NetCDF-4 file can be processed. """
        expected_catalog = Catalog('output', 'description')
        zarr_store = MagicMock()  # fsspec.mapping.FSMap

        mock_make_localstack.return_value = MagicMock(spec=S3FileSystem)
        mock_make_localstack.return_value.get_mapper.return_value = zarr_store
        mock_download.return_value = ['local_path.nc4']
        mock_get_catalog.return_value = expected_catalog

        granule_url = 'https://example.com/amazing_file.nc4'
        stac_catalog = Catalog('single_input', 'description of test catalog')
        stac_item = Item('id1', self.bbox_geometry, self.bbox,
                         self.granule_datetime, {})
        stac_item.add_asset(
            'data', Asset(granule_url, roles=['data'],
                          media_type='application/x-netcdf4')
        )
        stac_catalog.add_item(stac_item)

        message_content = self.base_message_content.copy()
        message_content['format'] = {'mime': 'application/x-zarr'}
        harmony_message = Message(message_content)
        harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                              catalog=stac_catalog,
                                              config=self.harmony_config)

        self.assertEqual(harmony_adapter.process_items_many_to_one(),
                         expected_catalog)

        # Assert the mocked functions were called with the expected arguments:
        mock_download.assert_called_once(list(stac_catalog.get_items()))
        mock_netcdf_to_zarr.assert_called_once_with('local_path.nc4', zarr_store)
        mock_get_catalog.assert_called_once_with(stac_catalog, zarr_store)

    @patch('harmony_netcdf_to_zarr.adapter.get_output_catalog')
    @patch('harmony_netcdf_to_zarr.adapter.netcdf_to_zarr')
    @patch('harmony_netcdf_to_zarr.adapter.download_granules')
    @patch('harmony_netcdf_to_zarr.adapter.make_localstack_s3fs')
    def process_items_multiple(self, mock_make_localstack, mock_download,
                               mock_netcdf_to_zarr, mock_get_catalog):
        """ Ensure multiple input NetCDF-4 files are correctly processed. Prior
            to DAS-1379, this will raise a `NotImplementedError`.

        """
        zarr_store = MagicMock()  # fsspec.mapping.FSMap

        mock_make_localstack.return_value = MagicMock(spec=S3FileSystem)
        mock_make_localstack.return_value.get_mapper.return_value = zarr_store
        mock_download.return_value = ['local_path.nc4']

        granule_url = 'https://example.com/amazing_file.nc4'
        stac_catalog = Catalog('single_input', 'description of test catalog')
        stac_item_one = Item('id1', self.bbox_geometry, self.bbox,
                             self.granule_datetime, {})
        stac_item_two = Item('id2', self.bbox_geometry, self.bbox,
                             self.granule_datetime, {})
        for stac_item in [stac_item_one, stac_item_two]:
            stac_item.add_asset(
                'data', Asset(granule_url, roles=['data'],
                              media_type='application/x-netcdf4')
            )
            stac_catalog.add_item(stac_item)

        message_content = self.base_message_content.copy()
        message_content['format'] = {'mime': 'application/x-zarr'}
        harmony_message = Message(message_content)
        harmony_adapter = NetCDFToZarrAdapter(harmony_message,
                                              catalog=stac_catalog,
                                              config=self.harmony_config)

        with self.assertRaises(NotImplementedError):
            harmony_adapter.process_items_many_to_one()

        mock_netcdf_to_zarr.assert_not_called()
        mock_get_catalog.assert_not_called()
