""" Unit tests for the `harmony_netcdf_to_zarr.stac_utilities` module. """
from datetime import datetime
from unittest import TestCase

from harmony.util import bbox_to_geometry
from pystac import Asset, Catalog, Item

from harmony_netcdf_to_zarr.stac_utilities import (get_item_date_range,
                                                   get_item_url,
                                                   get_netcdf_urls,
                                                   get_output_bounding_box,
                                                   get_output_catalog,
                                                   get_output_date_range,
                                                   get_output_item,
                                                   is_netcdf_asset)


class TestStacUtilities(TestCase):
    @classmethod
    def setUpClass(cls):
        """ Set test fixtures that can be defined once for all tests. """
        cls.bbox_one = [-40, -30, -20, -10]
        cls.bbox_two = [10, 20, 30, 40]
        cls.combined_bbox = [-40, -30, 30, 40]
        cls.bbox_one_geometry = bbox_to_geometry(cls.bbox_one)
        cls.bbox_two_geometry = bbox_to_geometry(cls.bbox_two)
        cls.combined_bbox_geometry = bbox_to_geometry(cls.combined_bbox)

        cls.datetime_one = datetime(1969, 7, 20, 17, 44, 0)
        cls.datetime_two = datetime(1969, 7, 20, 20, 17, 40)
        cls.datetime_three = datetime(1969, 7, 21, 17, 54, 0)
        cls.datetime_four = datetime(1969, 7, 21, 21, 35, 0)
        cls.datetime_one_str = '1969-07-20T17:44:00'
        cls.datetime_two_str = '1969-07-20T20:17:40'
        cls.datetime_three_str = '1969-07-21T17:54:00'
        cls.datetime_four_str = '1969-07-21T21:35:00'

    def test_get_catalog_urls(self):
        """ Ensure URLs can be correctly extracted from a list of STAC catalog
            items. If any items do not have a URL, then an exception should be
            raised.

        """
        item_one = Item('test', self.bbox_one_geometry, self.bbox_one,
                        self.datetime_one, {})
        item_two = item_one.clone()
        item_three = item_one.clone()

        url_one = 'data_one.nc4'
        url_two = 'data_two.nc4'

        item_one.add_asset('data', Asset(url_one, roles=['data'],
                                         media_type='application/x-netcdf4'))
        item_two.add_asset('data', Asset(url_two, roles=['data'],
                                         media_type='application/x-netcdf4'))
        item_three.add_asset('data', Asset('thumbnail.jpg', roles=['thumbnail'],
                                           media_type='image/jpg'))

        with self.subTest('All valid inputs returns expected list of URLs'):
            self.assertListEqual(get_netcdf_urls([item_one, item_two]),
                                 [url_one, url_two])

        with self.subTest('An item with no NetCDF-4 asset raises an exception'):
            with self.assertRaises(RuntimeError):
                get_netcdf_urls([item_one, item_two, item_three])

    def test_get_item_url(self):
        """ Ensure the URL for the first asset with a role of 'data' is
            returned. This function should also handle edge-cases including:

            * No assets.
            * No assets with the 'data' role.
            * Assets with no roles at all.
            * No assets with the correct media type.

        """
        base_item = Item('test', self.bbox_one_geometry, self.bbox_one,
                         self.datetime_one, {})
        data_url = 'https://example.com/shiny_data.nc4'
        nc4_media_type = 'application/x-netcdf4'

        with self.subTest('Gets first valid URL'):
            item = base_item.clone()
            asset = Asset(data_url, roles=['data'], media_type=nc4_media_type)
            item.add_asset('data', asset)
            self.assertEqual(get_item_url(item), data_url)

        with self.subTest('No assets returns None'):
            self.assertIsNone(get_item_url(base_item))

        with self.subTest('No "data" assets returns None'):
            item = base_item.clone()
            asset = Asset(data_url, roles=['thumbnail'], media_type=nc4_media_type)
            item.add_asset('data', asset)
            self.assertIsNone(get_item_url(base_item))

        with self.subTest('Assets with no roles at all return None'):
            item = base_item.clone()
            asset = Asset(data_url, media_type=nc4_media_type)
            item.add_asset('data', asset)
            self.assertIsNone(get_item_url(item))

        with self.subTest('No NetCDF-4 assets returns None'):
            item = base_item.clone()
            asset = Asset(data_url, roles=['data'], media_type='random')
            item.add_asset('data', asset)
            self.assertIsNone(get_item_url(item))

    def test_get_output_catalog(self):
        """ Ensure the return from this function includes only the Zarr store
            created by the service, which combines the spatial and temporal
            information from the inputs.

        """
        input_item_one = Item('id1', self.bbox_one_geometry, self.bbox_one,
                              self.datetime_three, {})
        input_item_two = Item('id2', self.bbox_two_geometry, self.bbox_two,
                              self.datetime_four, {})
        input_catalog = Catalog('test_input', 'this is a catalog for testing')
        input_catalog.add_item(input_item_one)
        input_catalog.add_item(input_item_two)

        zarr_root = 's3://bucket/path/to/output.zarr'

        output_catalog = get_output_catalog(input_catalog, zarr_root)

        # Ensure the output catalog has the same description as the input, but
        # a different ID:
        self.assertNotEqual(input_catalog.id, output_catalog.id)
        self.assertEqual(input_catalog.description, output_catalog.description)

        # Ensure the output catalog has only the output Zarr store item:
        output_items = list(output_catalog.get_items())
        self.assertEqual(len(output_items), 1)

        # Confirm the output STAC item is the Zarr store, and it combined the
        # spatial and temporal values from the input granules:
        self.assertListEqual(output_items[0].bbox, self.combined_bbox)
        self.assertIsNone(output_items[0].datetime)
        self.assertEqual(output_items[0].common_metadata.start_datetime,
                         self.datetime_three)
        self.assertEqual(output_items[0].common_metadata.end_datetime,
                         self.datetime_four)

    def test_get_output_item(self):
        """ Ensure that a single `pystac.Item` is created that combines the
            spatial and temporal extents of the supplied input items.

        """
        input_item_one = Item('id1', self.bbox_one_geometry, self.bbox_one,
                              self.datetime_three, {})
        input_item_two = Item('id2', self.bbox_two_geometry, self.bbox_two,
                              self.datetime_four, {})

        zarr_root = 's3://bucket/path/to/output.zarr'

        output_item = get_output_item([input_item_one, input_item_two], zarr_root)

        # Check the main `pystac.Item`:
        self.assertListEqual(output_item.bbox, self.combined_bbox)
        self.assertIsNone(output_item.datetime)
        self.assertEqual(output_item.common_metadata.start_datetime,
                         self.datetime_three)
        self.assertEqual(output_item.common_metadata.end_datetime,
                         self.datetime_four)

        # Ensure there is a single `pystac.Asset` listed under the 'data' key:
        self.assertEqual(len(output_item.assets), 1)
        self.assertListEqual(list(output_item.assets.keys()), ['data'])

        # Check the properties of the new `pystac.Asset`:
        output_asset = output_item.assets.get('data')
        self.assertEqual(output_asset.href, zarr_root)
        self.assertEqual(output_asset.title, 'output.zarr')  # basename of URL
        self.assertEqual(output_asset.media_type, 'application/x-zarr')
        self.assertEqual(output_asset.roles, ['data'])

    def test_get_output_bbox(self):
        """ Ensure a bounding box that encompasses the entire extents of all
            input granules is retrieved from the input STAC items. This
            function should also work for a single input STAC item.

        """
        properties = {'start_datetime': self.datetime_one_str,
                      'end_datetime': self.datetime_two_str}

        item_one = Item('id1', self.bbox_one_geometry, self.bbox_one, None,
                        properties)
        item_two = Item('id2', self.bbox_two_geometry, self.bbox_two, None,
                        properties)

        with self.subTest('Single input item, output matches input'):
            self.assertListEqual(get_output_bounding_box([item_one]),
                                 self.bbox_one)

        with self.subTest('Multiple input items, output encompasses all'):
            self.assertListEqual(get_output_bounding_box([item_one, item_two]),
                                 self.combined_bbox)

    def test_get_output_date_range(self):
        """ Ensure a date range that covers all input granules can be
            determined from the input STAC items. These may specify the time
            range via either a single datetime, or by metadata contain a
            start_datetime and end_datetime.

        """
        dt_item_one = Item('id1', self.bbox_one_geometry, self.bbox_one,
                           self.datetime_one, {})
        dt_item_two = Item('id2', self.bbox_one_geometry, self.bbox_one,
                           self.datetime_two, {})
        start_end_item_one = Item(
            'id3', self.bbox_one_geometry, self.bbox_one, None,
            {'start_datetime': self.datetime_one_str,
             'end_datetime': self.datetime_two_str}
        )
        start_end_item_two = Item(
            'id4', self.bbox_one_geometry, self.bbox_one, None,
            {'start_datetime': self.datetime_three_str,
             'end_datetime': self.datetime_four_str}
        )

        with self.subTest('Single granule, start and end datetime'):
            self.assertDictEqual(get_output_date_range([start_end_item_one]),
                                 {'start_datetime': self.datetime_one_str,
                                  'end_datetime': self.datetime_two_str})

        with self.subTest('Single granule, only datetime'):
            self.assertDictEqual(get_output_date_range([dt_item_one]),
                                 {'start_datetime': self.datetime_one_str,
                                  'end_datetime': self.datetime_one_str})

        with self.subTest('Multiple granules, start and end datetimes'):
            self.assertDictEqual(
                get_output_date_range([start_end_item_one, start_end_item_two]),
                {'start_datetime': self.datetime_one_str,
                 'end_datetime': self.datetime_four_str}
            )

        with self.subTest('Multiple granules, only datetime'):
            self.assertDictEqual(
                get_output_date_range([dt_item_one, dt_item_two]),
                {'start_datetime': self.datetime_one_str,
                 'end_datetime': self.datetime_two_str}
            )

    def test_get_item_date_range(self):
        """ Ensure that the date range for a STAC item can be retrieved,
            whether it is included as the single `Item.datetime` attribute or
            a `start_datetime` and `end_datetime`.

            For an item with only a single datetime, that value should be used
            for both the start and stop datetime.

        """
        datetime_item = Item('id1', self.bbox_one_geometry, self.bbox_one,
                             self.datetime_one, {})

        start_end_item = Item('id2', self.bbox_one_geometry, self.bbox_one,
                              None, {'start_datetime': self.datetime_one_str,
                                     'end_datetime': self.datetime_two_str})

        with self.subTest('Only datetime'):
            self.assertTupleEqual(get_item_date_range(datetime_item),
                                  (self.datetime_one, self.datetime_one))

        with self.subTest('Start and end datetimes'):
            self.assertTupleEqual(get_item_date_range(start_end_item),
                                  (self.datetime_one, self.datetime_two))

    def test_is_netcdf_asset(self):
        """ Ensure that a NetCDF-4 asset can be correctly identified via either
            the asset media type or the file extension. The check on the file
            extension should handle both uppercase and lowercase.

        """
        test_args = [['NetCDF-4 media type', 'application/x-netcdf4', '.h5'],
                     ['NetCDF media type', 'application/x-netcdf', '.nc4'],
                     ['HDF-5 media type', 'application/x-hdf5', '.h5'],
                     ['.nc4 extension', None, '.nc4'],
                     ['.nc extension', None, '.nc'],
                     ['.h5 extension', None, '.h5'],
                     ['.hdf5 extension', None, '.hdf5'],
                     ['.hdf extension', None, '.hdf'],
                     ['.HDF5 extension', None, '.HDF5']]

        for description, media_type, extension in test_args:
            with self.subTest(description):
                test_asset = Asset(f'test{extension}', media_type=media_type)
                self.assertTrue(is_netcdf_asset(test_asset))

        bad_args = [['Bad media-type', 'application/tiff', '.tiff'],
                    ['Missing media-type, bad extension', None, '.tiff']]

        for description, media_type, extension in bad_args:
            with self.subTest(description):
                test_asset = Asset(f'test{extension}', media_type=media_type)
                self.assertFalse(is_netcdf_asset(test_asset))
