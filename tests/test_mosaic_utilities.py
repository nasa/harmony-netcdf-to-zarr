""" Tests the `harmony_netcdf_to_zarr.mosaic_utilities` module. """
from datetime import datetime, timedelta
from unittest.mock import patch
from unittest import TestCase

from netCDF4 import Dataset
import numpy as np

from harmony_netcdf_to_zarr.mosaic_utilities import (DimensionInformation,
                                                     DimensionsMapping,
                                                     get_nc_attribute,
                                                     is_variable_in_dataset,
                                                     resolve_reference_path)


class TestMosaicUtilities(TestCase):
    @classmethod
    def setUpClass(cls):
        """ Set test fixtures that can be defined once for all tests. """
        cls.test_dataset_path = 'test_dataset.nc'
        cls.test_epoch = datetime(2020, 1, 27, 14, 0, 0)

    def setUp(self):
        """ Set test fixtures that need to be defined once per test. """
        self.test_dataset = self.generate_netcdf_input(self.test_dataset_path)

    def tearDown(self):
        """ Remove Dataset test fixture between tests. The `DimensionsMapping`
            class will close each `Dataset` it parses, so must check if the
            `Dataset` is still open before trying to close it.

        """
        if self.test_dataset.isopen():
            self.test_dataset.close()

    @staticmethod
    def generate_netcdf_input(dataset_name):
        """ Generate a NetCDF-4 file to be used in unit tests. This will have
            structure:

            |- latitude (1-D dimension variable)
            |- longitude (1-D dimension variable)
            |- time (1-D dimension variable)
            |- flat_variable (3-D gridded variable)
            |- science_group (group)
            |  |- nested (3-D gridded variable)

        """
        dataset = Dataset(dataset_name, diskless=True, mode='w')

        lat_data = np.linspace(-90, 90, 19)
        lon_data = np.linspace(-180, 180, 37)
        time_data = np.array([30.0])
        flat_data = np.ones((time_data.size, lat_data.size, lon_data.size, ))
        nested_data = np.ones_like(flat_data)

        grid_dimensions = ('time', 'latitude', 'longitude', )

        dataset.createDimension('latitude', size=lat_data.size)
        dataset.createDimension('longitude', size=lon_data.size)
        dataset.createDimension('time', size=time_data.size)

        latitude = dataset.createVariable('latitude', lat_data.dtype,
                                          dimensions=('latitude', ))
        latitude[:] = lat_data
        latitude.setncattr('units', 'degrees_north')

        longitude = dataset.createVariable('longitude', lon_data.dtype,
                                           dimensions=('longitude', ))
        longitude[:] = lon_data
        longitude.setncattr('units', 'degrees_east')

        time = dataset.createVariable('time', time_data.dtype,
                                      dimensions=('time', ))
        time[:] = time_data
        time.setncattr('units', 'seconds since 2020-01-27T14:00:00')

        flat = dataset.createVariable('flat_variable', flat_data.dtype,
                                      dimensions=grid_dimensions)
        flat[:] = flat_data

        nested = dataset.createVariable('/science_group/nested',
                                        nested_data.dtype,
                                        dimensions=grid_dimensions)
        nested[:] = nested_data

        return dataset

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping(self, mock_dataset):
        """ Ensure the test granule is successfully parsed and that all
            dimensions are extracted. There should only be a single entry for
            each dimension, even though multiple variables refer to that
            dimension.

            In this test a list of two NetCDF-4 Dataset will be used, to
            confirm the input data mapping supports multiple input files. The
            second input NetCDF-4 Dataset will have identical spatial
            dimensions, but a different value in the time dimension.

        """
        second_dataset = self.generate_netcdf_input('second_dataset.nc')
        second_dataset['/time'][:] = np.array([60.0])

        # Have to mock `netCDF4.Dataset` responses, as they are only in-memory.
        mock_dataset.side_effect = [self.test_dataset, second_dataset]

        input_datasets = [self.test_dataset_path, 'second_dataset.nc']
        dimensions_mapping = DimensionsMapping(input_datasets)

        # Ensure both NetCDF-4 datasets were parsed:
        self.assertEqual(mock_dataset.call_count, 2)
        mock_dataset.assert_any_call(self.test_dataset_path, 'r')
        mock_dataset.assert_any_call('second_dataset.nc', 'r')

        # Ensure all dimensions are detected from input datasets:
        expected_dimensions = {'/latitude', '/longitude', '/time'}
        self.assertSetEqual(set(dimensions_mapping.input_dimensions.keys()),
                            expected_dimensions)

        # Ensure each dimension has information from each input dataset:
        for dimension in expected_dimensions:
            self.assertSetEqual(
                set(dimensions_mapping.input_dimensions[dimension].keys()),
                set(input_datasets)
            )

        # Check each dimension matches the expectation from the input file
        latitude_dimension = dimensions_mapping.input_dimensions['/latitude']
        expected_latitude_values = np.linspace(-90, 90, 19)
        self.assertIsNone(latitude_dimension[self.test_dataset_path].epoch)
        self.assertIsNone(latitude_dimension['second_dataset.nc'].epoch)
        self.assertIsNone(latitude_dimension[self.test_dataset_path].time_unit)
        self.assertIsNone(latitude_dimension['second_dataset.nc'].time_unit)
        np.testing.assert_array_equal(
            latitude_dimension[self.test_dataset_path].values,
            expected_latitude_values
        )
        np.testing.assert_array_equal(
            latitude_dimension['second_dataset.nc'].values,
            expected_latitude_values
        )

        longitude_dimension = dimensions_mapping.input_dimensions['/longitude']
        expected_longitude_values = np.linspace(-180, 180, 37)
        self.assertIsNone(longitude_dimension[self.test_dataset_path].epoch)
        self.assertIsNone(longitude_dimension['second_dataset.nc'].epoch)
        self.assertIsNone(longitude_dimension[self.test_dataset_path].time_unit)
        self.assertIsNone(longitude_dimension['second_dataset.nc'].time_unit)
        np.testing.assert_array_equal(
            longitude_dimension[self.test_dataset_path].values,
            expected_longitude_values
        )
        np.testing.assert_array_equal(
            longitude_dimension['second_dataset.nc'].values,
            expected_longitude_values
        )

        time_dimension = dimensions_mapping.input_dimensions['/time']
        self.assertEqual(time_dimension[self.test_dataset_path].epoch,
                         self.test_epoch)
        self.assertEqual(time_dimension['second_dataset.nc'].epoch,
                         self.test_epoch)
        self.assertEqual(time_dimension[self.test_dataset_path].time_unit,
                         timedelta(seconds=1))
        self.assertEqual(time_dimension['second_dataset.nc'].time_unit,
                         timedelta(seconds=1))
        np.testing.assert_array_equal(
            time_dimension[self.test_dataset_path].values,
            np.array([30.0])
        )
        np.testing.assert_array_equal(
            time_dimension['second_dataset.nc'].values,
            np.array([60.0])
        )

    def test_dimension_information(self):
        """ Ensure a dimension variable will have the expected information
            extracted from the NetCDF-4 file, including path, values,
            units metadata and and temporal unit or epoch information. A non-
            temporal dimension should populate those last two items with `None`
            values.

        """
        with self.subTest('A non-temporal dimension is extract'):
            spatial_dimension = DimensionInformation(self.test_dataset,
                                                     '/longitude')
            self.assertEqual(spatial_dimension.dimension_path, '/longitude')
            self.assertEqual(spatial_dimension.units, 'degrees_east')
            self.assertIsNone(spatial_dimension.epoch)
            self.assertIsNone(spatial_dimension.time_unit)
            np.testing.assert_array_equal(spatial_dimension.values,
                                          self.test_dataset['/longitude'][:])

        with self.subTest('A temporal dimension is extracted'):
            time_dimension = DimensionInformation(self.test_dataset, '/time')
            self.assertEqual(time_dimension.dimension_path, '/time')
            self.assertEqual(time_dimension.units,
                             'seconds since 2020-01-27T14:00:00')
            self.assertEqual(time_dimension.epoch, self.test_epoch)
            self.assertEqual(time_dimension.time_unit, timedelta(seconds=1))
            np.testing.assert_array_equal(time_dimension.values,
                                          self.test_dataset['/time'][:])

    def test_get_nc_attribute(self):
        """ Ensure the helper function wrapping the `getncattr` class method
            can handle present and absent attributes, including when a
            default value is supplied or omitted.

        """
        with self.subTest('Attribute is present'):
            self.assertEqual(
                get_nc_attribute(
                    self.test_dataset['/time'], 'units'
                ),
                'seconds since 2020-01-27T14:00:00'
            )

        with self.subTest('Attribute is present, ignores default value'):
            self.assertEqual(
                get_nc_attribute(
                    self.test_dataset['/time'], 'units', 'default'
                ),
                'seconds since 2020-01-27T14:00:00'
            )

        with self.subTest('Attribute is absent, and no default specified.'):
            self.assertEqual(
                get_nc_attribute(
                    self.test_dataset['/time'], 'absent_attribute'
                ),
                None
            )

        with self.subTest('Attribute is absent, uses specified default.'):
            self.assertEqual(
                get_nc_attribute(
                    self.test_dataset['/time'], 'absent_attribute', 'default'
                ),
                'default'
            )

    def test_resolve_reference_path(self):
        """ Ensure a reference from variable metadata is correctly qualified to
            a full variable path.

        """
        with self.subTest('Reference is an absolute path'):
            self.assertEqual(
                resolve_reference_path(
                    self.test_dataset['/science_group/nested'], '/time'
                ),
                '/time'
            )

        with self.subTest('Unresolved path to nested variable'):
            self.assertEqual(
                resolve_reference_path(
                    self.test_dataset['/science_group/nested'], 'nested'
                ),
                '/science_group/nested'
            )

        with self.subTest('Unresolved root variable from nested group.'):
            self.assertEqual(
                resolve_reference_path(
                    self.test_dataset['/science_group/nested'], 'time'
                ),
                '/time'
            )

        with self.subTest('Unresolved root variable from root variable.'):
            self.assertEqual(
                resolve_reference_path(self.test_dataset['/latitude'], 'time'),
                '/time'
            )

    def test_is_variable_in_dataset(self):
        """ Ensure a variable can be correctly identified in a NetCDF-4
            dataset. This should work for variables in both the root dataset
            group and also nested variables.

        """
        with self.subTest('Root group variable present'):
            self.assertTrue(is_variable_in_dataset('/flat_variable',
                                                   self.test_dataset))

        with self.subTest('Nested variable present'):
            self.assertTrue(is_variable_in_dataset('/science_group/nested',
                                                   self.test_dataset))

        with self.subTest('Absent root group variable'):
            self.assertFalse(is_variable_in_dataset('/missing_variable',
                                                    self.test_dataset))

        with self.subTest('Absent nested variable'):
            self.assertFalse(is_variable_in_dataset('/science_group/missing',
                                                    self.test_dataset))

        with self.subTest('Absent group in path'):
            self.assertFalse(is_variable_in_dataset('/missing_group/variable',
                                                    self.test_dataset))
