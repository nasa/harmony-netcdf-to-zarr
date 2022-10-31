""" Tests the `harmony_netcdf_to_zarr.mosaic_utilities` module. """
from datetime import datetime, timedelta
from unittest.mock import call, patch
from unittest import TestCase

from netCDF4 import Dataset
import numpy as np
from numpy.testing import assert_array_equal

from harmony_netcdf_to_zarr.mosaic_utilities import (DimensionInformation,
                                                     DimensionsMapping,
                                                     get_grid_values,
                                                     get_nc_attribute,
                                                     get_resolution,
                                                     is_variable_in_dataset,
                                                     NetCDF4DimensionInformation,
                                                     resolve_reference_path,
                                                     scale_to_integers)


class TestMosaicUtilities(TestCase):
    @classmethod
    def setUpClass(cls):
        """ Set test fixtures that can be defined once for all tests. """
        cls.lat_data = np.linspace(-90, 90, 19)
        cls.lon_data = np.linspace(-180, 180, 37)
        cls.test_dataset_path = 'test_dataset.nc'
        cls.temporal_units = 'seconds since 2020-01-27T14:00:00'
        cls.test_epoch = datetime(2020, 1, 27, 14, 0, 0)

    def setUp(self):
        """ Set test fixtures that need to be defined once per test. """
        self.test_dataset = self.generate_netcdf_input(self.test_dataset_path,
                                                       self.lat_data,
                                                       self.lon_data,
                                                       np.array([30.0]),
                                                       self.temporal_units)

    def tearDown(self):
        """ Remove Dataset test fixture between tests. The `DimensionsMapping`
            class will close each `Dataset` it parses, so must check if the
            `Dataset` is still open before trying to close it.

        """
        if self.test_dataset.isopen():
            self.test_dataset.close()

    @staticmethod
    def generate_netcdf_input(dataset_name: str, lat_data: np.ndarray,
                              lon_data: np.ndarray, time_data: np.ndarray,
                              time_units: str):
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
        time.setncattr('units', time_units)

        flat = dataset.createVariable('flat_variable', flat_data.dtype,
                                      dimensions=grid_dimensions)
        flat[:] = flat_data

        nested = dataset.createVariable('/science_group/nested',
                                        nested_data.dtype,
                                        dimensions=grid_dimensions)
        nested[:] = nested_data

        return dataset

    @staticmethod
    def generate_netcdf_with_bounds(dataset_name: str,
                                    dimension_name: str,
                                    dimension_data: np.ndarray,
                                    bounds_data: np.ndarray) -> Dataset:
        """ Generate a NetCDF-4 file to be used in unit tests with a dimension
            variable and an associated bounds variable.

            |- dimension_name (1-D dimension variable, N elements)
            |- dimension_name_bnds (2-D bounds variable, N x 2 elements)

        """
        bounds_name = f'{dimension_name}_bnds'

        bounds_dataset = Dataset(dataset_name, diskless=True, mode='w')
        bounds_dataset.createDimension('nv', size=2)
        bounds_dataset.createDimension(dimension_name,
                                       size=dimension_data.size)
        dimension = bounds_dataset.createVariable(dimension_name,
                                                  dimension_data.dtype,
                                                  dimensions=(dimension_name, ))
        dimension[:] = dimension_data
        dimension.setncattr('bounds', bounds_name)
        dimension.setncattr('units', 'seconds since 2001-01-01T00:00:00')

        bounds = bounds_dataset.createVariable(bounds_name, bounds_data.dtype,
                                               dimensions=(dimension_name, 'nv'))
        bounds[:] = bounds_data
        bounds.setncattr('units', 'seconds since 2001-01-01T00:00:00')

        return bounds_dataset

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping_input(self, mock_dataset):
        """ Ensure the test granule is successfully parsed and that all
            dimensions are extracted. There should only be a single entry for
            each dimension, even though multiple variables refer to that
            dimension.

            In this test a list of two NetCDF-4 Dataset will be used, to
            confirm the input data mapping supports multiple input files. The
            second input NetCDF-4 Dataset will have identical spatial
            dimensions, but a different value in the time dimension.

        """
        second_dataset = self.generate_netcdf_input('second_dataset.nc',
                                                    self.lat_data,
                                                    self.lon_data,
                                                    np.array([60.0]),
                                                    self.temporal_units)

        # Have to mock `netCDF4.Dataset` responses, as they are only in-memory.
        mock_dataset.side_effect = [self.test_dataset, second_dataset]

        input_datasets = [self.test_dataset_path, 'second_dataset.nc']
        dimensions_mapping = DimensionsMapping(input_datasets)

        # Ensure both NetCDF-4 datasets were parsed:
        self.assertEqual(mock_dataset.call_count, 2)
        mock_dataset.assert_has_calls([call(self.test_dataset_path, 'r'),
                                       call('second_dataset.nc', 'r')])

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
        self.assertIsNone(latitude_dimension[self.test_dataset_path].epoch)
        self.assertIsNone(latitude_dimension['second_dataset.nc'].epoch)
        self.assertIsNone(latitude_dimension[self.test_dataset_path].time_unit)
        self.assertIsNone(latitude_dimension['second_dataset.nc'].time_unit)
        assert_array_equal(latitude_dimension[self.test_dataset_path].values,
                           self.lat_data)
        assert_array_equal(latitude_dimension['second_dataset.nc'].values,
                           self.lat_data)

        longitude_dimension = dimensions_mapping.input_dimensions['/longitude']
        self.assertIsNone(longitude_dimension[self.test_dataset_path].epoch)
        self.assertIsNone(longitude_dimension['second_dataset.nc'].epoch)
        self.assertIsNone(longitude_dimension[self.test_dataset_path].time_unit)
        self.assertIsNone(longitude_dimension['second_dataset.nc'].time_unit)
        assert_array_equal(longitude_dimension[self.test_dataset_path].values,
                           self.lon_data)
        assert_array_equal(longitude_dimension['second_dataset.nc'].values,
                           self.lon_data)

        time_dimension = dimensions_mapping.input_dimensions['/time']
        self.assertEqual(time_dimension[self.test_dataset_path].epoch,
                         self.test_epoch)
        self.assertEqual(time_dimension['second_dataset.nc'].epoch,
                         self.test_epoch)
        self.assertEqual(time_dimension[self.test_dataset_path].time_unit,
                         timedelta(seconds=1))
        self.assertEqual(time_dimension['second_dataset.nc'].time_unit,
                         timedelta(seconds=1))
        assert_array_equal(time_dimension[self.test_dataset_path].values,
                           np.array([30.0]))
        assert_array_equal(time_dimension['second_dataset.nc'].values,
                           np.array([60.0]))

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping_single_input(self, mock_dataset):
        """ Ensure that, when a single input is given to `DimensionsMapping`,
            the class will not try to create a regularly spaced dimension
            instead of the input dimensions. This ensures for a single input
            granule, the output Zarr exactly matches the input Zarr.

        """
        # Have to mock `netCDF4.Dataset` responses, as they are only in-memory.
        mock_dataset.return_value = self.test_dataset

        input_datasets = [self.test_dataset_path]
        dimensions_mapping = DimensionsMapping(input_datasets)

        # Ensure the NetCDF-4 dataset was parsed:
        mock_dataset.assert_called_once_with(self.test_dataset_path, 'r')

        # Ensure all dimensions are detected from the input dataset:
        expected_dimensions = {'/latitude', '/longitude', '/time'}
        self.assertSetEqual(set(dimensions_mapping.input_dimensions.keys()),
                            expected_dimensions)

        # Ensure no aggregated output dimensions were derived:
        self.assertDictEqual(dimensions_mapping.output_dimensions, {})

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping_output_merra(self, mock_dataset):
        """ Test that the `DimensionsMapping.output_dimensions` mapping is
            correctly instantiated from known input data. This specific test
            targets data like MERRA-2, where the spatial dimensions are the
            same in each granule, the temporal dimension values are the same,
            but the temporal dimension epochs vary between granules.

            The two subtests are:

            * Continuous granules (e.g., all output dimension values map to an
              input value).
            * Discontinous granules (e.g., there is intervening space in the
              output temporal dimension).

        """
        merra_time_values = np.linspace(0, 1380, 24)
        dataset_one = self.generate_netcdf_input(
            'merra_one.nc4', self.lat_data, self.lon_data,
            merra_time_values, 'minutes since 2020-01-01T00:30:00'
        )
        dataset_two = self.generate_netcdf_input(
            'merra_two.nc4', self.lat_data, self.lon_data,
            merra_time_values, 'minutes since 2020-01-02T00:30:00'
        )
        dataset_three = self.generate_netcdf_input(
            'merra_three.nc4', self.lat_data, self.lon_data,
            merra_time_values, 'minutes since 2020-01-03T00:30:00'
        )
        dataset_four = self.generate_netcdf_input(
            'merra_four.nc4', self.lat_data, self.lon_data,
            merra_time_values, 'minutes since 2020-01-05T00:30:00'
        )

        with self.subTest('Continuous MERRA-2 granules'):
            mock_dataset.side_effect = [dataset_one, dataset_two]
            merra_mapping = DimensionsMapping(['merra_one.nc4',
                                               'merra_two.nc4'])

            # Check the expected dimensions are in the output mapping.
            # Note: aggregation of non-temporal dimensions has been disabled
            # as the Swath Projector can have values with slight rounding
            # errors in their output grid dimensions.
            self.assertSetEqual(set(merra_mapping.output_dimensions.keys()),
                                {'/time'})
            """
            self.assertSetEqual(set(merra_mapping.output_dimensions.keys()),
                                {'/time', '/latitude', '/longitude'})

            # Check the output latitude has correct values and units.
            self.assertEqual(
                merra_mapping.output_dimensions['/latitude'].units,
                'degrees_north'
            )
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].epoch)
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].time_unit)
            assert_array_equal(merra_mapping.output_dimensions['/latitude'].values,
                               self.lat_data)

            # Check the output longitude has correct values and units.
            self.assertEqual(
                merra_mapping.output_dimensions['/longitude'].units,
                'degrees_east'
            )
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].epoch)
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].time_unit)
            assert_array_equal(merra_mapping.output_dimensions['/longitude'].values,
                               self.lon_data)
            """

            # Check the output time has correct values and units.
            self.assertEqual(merra_mapping.output_dimensions['/time'].units,
                             'minutes since 2020-01-01T00:30:00')
            assert_array_equal(merra_mapping.output_dimensions['/time'].values,
                               np.linspace(0, 2820, 48))  # 48 values of consecutive hours

            # Check none of the output dimensions have bounds information, as
            # none of the inputs did.
            """
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].bounds_values)
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].bounds_path)
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].bounds_values)
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].bounds_path)
            """
            self.assertIsNone(merra_mapping.output_dimensions['/time'].bounds_values)
            self.assertIsNone(merra_mapping.output_dimensions['/time'].bounds_path)

        with self.subTest('MERRA-2 data with a gap between granules.'):
            mock_dataset.side_effect = [dataset_three, dataset_four]
            merra_mapping = DimensionsMapping(['merra_three.nc4',
                                               'merra_four.nc4'])

            # Check the expected dimensions are in the output mapping.
            # Note: aggregation of non-temporal dimensions has been disabled
            # as the Swath Projector can have values with slight rounding
            # errors in their output grid dimensions.
            self.assertSetEqual(set(merra_mapping.output_dimensions.keys()),
                                {'/time'})

            """
            self.assertSetEqual(set(merra_mapping.output_dimensions.keys()),
                                {'/time', '/latitude', '/longitude'})

            # Check the output latitude has correct values and units.
            self.assertEqual(
                merra_mapping.output_dimensions['/latitude'].units,
                'degrees_north'
            )
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].epoch)
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].time_unit)
            assert_array_equal(merra_mapping.output_dimensions['/latitude'].values,
                               self.lat_data)

            # Check the output longitude has correct values and units.
            self.assertEqual(
                merra_mapping.output_dimensions['/longitude'].units,
                'degrees_east'
            )
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].epoch)
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].time_unit)
            assert_array_equal(merra_mapping.output_dimensions['/longitude'].values,
                               self.lon_data)
            """

            # Check the output time has correct values and units.
            self.assertEqual(merra_mapping.output_dimensions['/time'].units,
                             'minutes since 2020-01-03T00:30:00')
            assert_array_equal(merra_mapping.output_dimensions['/time'].values,
                               np.linspace(0, 4260, 72))  # 72 values of consecutive hours

            # Check none of the output dimensions have bounds information, as
            # none of the inputs did.
            """
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].bounds_values)
            self.assertIsNone(merra_mapping.output_dimensions['/latitude'].bounds_path)
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].bounds_values)
            self.assertIsNone(merra_mapping.output_dimensions['/longitude'].bounds_path)
            """
            self.assertIsNone(merra_mapping.output_dimensions['/time'].bounds_values)
            self.assertIsNone(merra_mapping.output_dimensions['/time'].bounds_path)

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping_output_gpm(self, mock_dataset):
        """ Test that the `DimensionsMapping.output_dimensions` mapping is
            correctly instantiated from known input data. This specific test
            targets data like GPM/IMERG, where the spatial dimensions are the
            same in each granule, the temporal dimension epochs are the same,
            but the temporal dimension values vary between granules.

            The two subtests are:

            * Continuous granules (e.g., all output dimension values map to an
              input value).
            * Discontinous granules (e.g., there is intervening space in the
              output temporal dimension).

        """
        expected_output_time_values = np.linspace(0, 432000, 6)  # Daily data
        dataset_one = self.generate_netcdf_input(
            'gpm_one.nc4', self.lat_data, self.lon_data,
            np.array([expected_output_time_values[0]]), self.temporal_units
        )
        dataset_two = self.generate_netcdf_input(
            'gpm_two.nc4', self.lat_data, self.lon_data,
            np.array([expected_output_time_values[2]]), self.temporal_units
        )
        dataset_three = self.generate_netcdf_input(
            'gpm_three.nc4', self.lat_data, self.lon_data,
            np.array([expected_output_time_values[5]]), self.temporal_units
        )

        mock_dataset.side_effect = [dataset_one, dataset_two, dataset_three]
        gpm_mapping = DimensionsMapping(['gpm_one.nc4', 'gpm_two.nc4',
                                         'gpm_three.nc4'])

        # Check the expected dimensions are in the output mapping.
        # Note: aggregation of non-temporal dimensions has been disabled
        # as the Swath Projector can have values with slight rounding
        # errors in their output grid dimensions.
        self.assertSetEqual(set(gpm_mapping.output_dimensions.keys()),
                            {'/time'})
        """
        self.assertSetEqual(set(gpm_mapping.output_dimensions.keys()),
                            {'/time', '/latitude', '/longitude'})

        # Check the output latitude has correct values and units.
        self.assertEqual(
            gpm_mapping.output_dimensions['/latitude'].units,
            'degrees_north'
        )
        self.assertIsNone(gpm_mapping.output_dimensions['/latitude'].epoch)
        self.assertIsNone(gpm_mapping.output_dimensions['/latitude'].time_unit)
        assert_array_equal(gpm_mapping.output_dimensions['/latitude'].values,
                           self.lat_data)

        # Check the output longitude has correct values and units.
        self.assertEqual(
            gpm_mapping.output_dimensions['/longitude'].units,
            'degrees_east'
        )
        self.assertIsNone(gpm_mapping.output_dimensions['/longitude'].epoch)
        self.assertIsNone(gpm_mapping.output_dimensions['/longitude'].time_unit)
        assert_array_equal(gpm_mapping.output_dimensions['/longitude'].values,
                           self.lon_data)
        """

        # Check the output time has correct values and units.
        self.assertEqual(gpm_mapping.output_dimensions['/time'].units,
                         self.temporal_units)
        assert_array_equal(gpm_mapping.output_dimensions['/time'].values,
                           expected_output_time_values)

        # Check none of the output dimensions have bounds information, as
        # none of the inputs did.
        """
        self.assertIsNone(gpm_mapping.output_dimensions['/latitude'].bounds_values)
        self.assertIsNone(gpm_mapping.output_dimensions['/latitude'].bounds_path)
        self.assertIsNone(gpm_mapping.output_dimensions['/longitude'].bounds_values)
        self.assertIsNone(gpm_mapping.output_dimensions['/longitude'].bounds_path)
        """
        self.assertIsNone(gpm_mapping.output_dimensions['/time'].bounds_values)
        self.assertIsNone(gpm_mapping.output_dimensions['/time'].bounds_path)

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping_output_spatial(self, mock_dataset):
        """ Test that the `DimensionsMapping.output_dimensions` mapping is
            correctly instantiated from known input data. This specific test
            ensures non-temporal dimensions that do not overlap (e.g., two
            discontiguous tiles of the same grid) are correctly combined.

        """
        lat_data_one = np.array([-10, -5])
        lat_data_two = np.array([10, 15])
        lon_data_one = np.array([1, 2, 3])
        lon_data_two = np.array([6, 7])

        dataset_one = self.generate_netcdf_input(
            'spatial_one.nc4', lat_data_one, lon_data_one,
            np.array([0]), self.temporal_units
        )
        dataset_two = self.generate_netcdf_input(
            'spatial_two.nc4', lat_data_two, lon_data_two,
            np.array([0]), self.temporal_units
        )

        mock_dataset.side_effect = [dataset_one, dataset_two]
        spatial_mapping = DimensionsMapping(['spatial_one.nc4',
                                             'spatial_two.nc4'])

        # Check the expected dimensions are in the output mapping.
        # Note: aggregation of non-temporal dimensions has been disabled
        # as the Swath Projector can have values with slight rounding
        # errors in their output grid dimensions.
        # self.assertSetEqual(set(spatial_mapping.output_dimensions.keys()),
        #                     {'/time'})
        # expected_output_lat_data = np.array([-10, -5, 0, 5, 10, 15])
        # expected_output_lon_data = np.array([1, 2, 3, 4, 5, 6, 7])

        # self.assertSetEqual(set(spatial_mapping.output_dimensions.keys()),
        #                     {'/time', '/latitude', '/longitude'})

        # Check the output latitude has correct values and units.
        # self.assertEqual(
        #     spatial_mapping.output_dimensions['/latitude'].units,
        #     'degrees_north'
        # )
        # self.assertIsNone(spatial_mapping.output_dimensions['/latitude'].epoch)
        # self.assertIsNone(spatial_mapping.output_dimensions['/latitude'].time_unit)
        # assert_array_equal(spatial_mapping.output_dimensions['/latitude'].values,
        #                    expected_output_lat_data)

        # Check the output longitude has correct values and units.
        # self.assertEqual(
        #     spatial_mapping.output_dimensions['/longitude'].units,
        #     'degrees_east'
        # )
        # self.assertIsNone(spatial_mapping.output_dimensions['/longitude'].epoch)
        # self.assertIsNone(spatial_mapping.output_dimensions['/longitude'].time_unit)
        # assert_array_equal(spatial_mapping.output_dimensions['/longitude'].values,
        #                    expected_output_lon_data)

        # Check the output time has correct values and units.
        self.assertEqual(spatial_mapping.output_dimensions['/time'].units,
                         self.temporal_units)
        assert_array_equal(spatial_mapping.output_dimensions['/time'].values,
                           np.array([0]))

        # Check none of the output dimensions have bounds information, as
        # none of the inputs did.
        """
        self.assertIsNone(spatial_mapping.output_dimensions['/latitude'].bounds_values)
        self.assertIsNone(spatial_mapping.output_dimensions['/latitude'].bounds_path)
        self.assertIsNone(spatial_mapping.output_dimensions['/longitude'].bounds_values)
        self.assertIsNone(spatial_mapping.output_dimensions['/longitude'].bounds_path)
        """
        self.assertIsNone(spatial_mapping.output_dimensions['/time'].bounds_values)
        self.assertIsNone(spatial_mapping.output_dimensions['/time'].bounds_path)

    @patch('harmony_netcdf_to_zarr.mosaic_utilities.Dataset')
    def test_dimensions_mapping_bounds(self, mock_dataset):
        """ Ensure that when input dimensions have bounds information, the
            output dimensions also contain the correct bounds information.
            This should cover two cases:

            * All output dimension values map to an input dimension value
              and therefore all output bounds values can be copied from the
              input data
            * There are output dimension values that do not map to input
              dimension values (due to gaps in coverage) and the corresponding
              bounds values for those gaps must be calculated.

        """
        dimension_data_one = np.linspace(0, 2, 3)
        bounds_data_one = np.array([[-0.5, 0.5],
                                    [0.5, 1.5],
                                    [1.5, 2.5]])

        dimension_data_two = np.linspace(3, 5, 3)
        bounds_data_two = np.array([[2.5, 3.5],
                                    [3.5, 4.5],
                                    [4.5, 5.5]])

        dimension_data_three = np.linspace(9, 11, 3)
        bounds_data_three = np.array([[8.5, 9.5],
                                      [9.5, 10.5],
                                      [10.5, 11.5]])

        with self.subTest('All output dimension values have input bounds'):
            dataset_one = self.generate_netcdf_with_bounds('bounds_one.nc4',
                                                           'dim',
                                                           dimension_data_one,
                                                           bounds_data_one)

            dataset_two = self.generate_netcdf_with_bounds('bounds_two.nc4',
                                                           'dim',
                                                           dimension_data_two,
                                                           bounds_data_two)
            mock_dataset.side_effect = [dataset_one, dataset_two]

            mapping = DimensionsMapping([dataset_one, dataset_two])

            expected_output_bounds = np.array([[-0.5, 0.5],
                                               [0.5, 1.5],
                                               [1.5, 2.5],
                                               [2.5, 3.5],
                                               [3.5, 4.5],
                                               [4.5, 5.5]])

            self.assertDictEqual(mapping.output_bounds, {'/dim_bnds': '/dim'})
            assert_array_equal(mapping.output_dimensions['/dim'].values,
                               np.array([0, 1, 2, 3, 4, 5]))
            self.assertEqual(mapping.output_dimensions['/dim'].bounds_path,
                             '/dim_bnds')
            assert_array_equal(mapping.output_dimensions['/dim'].bounds_values,
                               expected_output_bounds)

            for dataset in [dataset_one, dataset_two]:
                if dataset.isopen():
                    dataset.close()

        with self.subTest('Some output dimension values are in coverage gaps'):
            dataset_one = self.generate_netcdf_with_bounds('bounds_three.nc4',
                                                           'dim',
                                                           dimension_data_one,
                                                           bounds_data_one)

            dataset_two = self.generate_netcdf_with_bounds('bounds_four.nc4',
                                                           'dim',
                                                           dimension_data_two,
                                                           bounds_data_two)

            dataset_three = self.generate_netcdf_with_bounds('bounds_five.nc4',
                                                             'dim',
                                                             dimension_data_three,
                                                             bounds_data_three)

            mock_dataset.side_effect = [dataset_one, dataset_two, dataset_three]

            mapping = DimensionsMapping([dataset_one, dataset_two, dataset_three])

            expected_output_bounds = np.array([[-0.5, 0.5],
                                               [0.5, 1.5],
                                               [1.5, 2.5],
                                               [2.5, 3.5],
                                               [3.5, 4.5],
                                               [4.5, 5.5],
                                               [5.5, 6.5],
                                               [6.5, 7.5],
                                               [7.5, 8.5],
                                               [8.5, 9.5],
                                               [9.5, 10.5],
                                               [10.5, 11.5]])

            assert_array_equal(mapping.output_dimensions['/dim'].values,
                               np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
            self.assertEqual(mapping.output_dimensions['/dim'].bounds_path,
                             '/dim_bnds')
            assert_array_equal(mapping.output_dimensions['/dim'].bounds_values,
                               expected_output_bounds)

            for dataset in [dataset_one, dataset_two, dataset_three]:
                if dataset.isopen():
                    dataset.close()

    def test_dimension_information(self):
        """ Ensure the base class can be instantiated with values, units and,
            where required, temporal unit and epoch information.

        """
        dimension_path = '/path/to/dimension'
        dimension_values = np.linspace(0, 5, 6)
        spatial_units = 'degrees_east'
        bounds_path = '/path/to/bounds'
        bounds_values = np.array([[-0.5, 0.5],
                                  [0.5, 1.5],
                                  [1.5, 2.5],
                                  [2.5, 3.5],
                                  [3.5, 4.5],
                                  [4.5, 5.5]])

        with self.subTest('A non-temporal dimension is extracted'):
            spatial_dimension = DimensionInformation(dimension_path,
                                                     dimension_values,
                                                     spatial_units)
            self.assertEqual(spatial_dimension.dimension_path, dimension_path)
            self.assertEqual(spatial_dimension.units, spatial_units)
            self.assertIsNone(spatial_dimension.epoch)
            self.assertIsNone(spatial_dimension.time_unit)
            assert_array_equal(spatial_dimension.values, dimension_values)

            # bounds are not defined, so values and path should be `None`:
            self.assertIsNone(spatial_dimension.bounds_values)
            self.assertIsNone(spatial_dimension.bounds_path)

        with self.subTest('A temporal dimension is extracted'):
            temporal_dimension = DimensionInformation(dimension_path,
                                                      dimension_values,
                                                      self.temporal_units)
            self.assertEqual(temporal_dimension.dimension_path, dimension_path)
            self.assertEqual(temporal_dimension.units, self.temporal_units)
            self.assertEqual(temporal_dimension.epoch, self.test_epoch)
            self.assertEqual(temporal_dimension.time_unit, timedelta(seconds=1))
            assert_array_equal(temporal_dimension.values, dimension_values)

            # bounds are not defined, so values and path should be `None`:
            self.assertIsNone(spatial_dimension.bounds_values)
            self.assertIsNone(spatial_dimension.bounds_path)

        with self.subTest('Bounds data are preserved when supplied'):
            bounds_dimension = DimensionInformation(dimension_path,
                                                    dimension_values,
                                                    spatial_units,
                                                    bounds_path,
                                                    bounds_values)
            self.assertEqual(bounds_dimension.dimension_path, dimension_path)
            self.assertEqual(bounds_dimension.units, spatial_units)
            self.assertIsNone(bounds_dimension.epoch)
            self.assertIsNone(bounds_dimension.time_unit)
            assert_array_equal(bounds_dimension.values, dimension_values)
            self.assertEqual(bounds_dimension.bounds_path, bounds_path)
            assert_array_equal(bounds_dimension.bounds_values, bounds_values)

    def test_dimension_information_get_values(self):
        """ Ensure the values can be retrieved from a `DimensionInformation`
            instance. If the dimension is non-temporal, or if an output `units`
            string is not specified, the values should be retrieved as they are
            stored in the class. For temporal dimensions where an output epoch
            has been supplied, the values should be updated to use that new
            epoch.

        """
        dimension_values = np.linspace(0, 1380, 24)
        input_temporal_units = 'minutes since 2021-01-02T00:30:00'
        output_temporal_units = 'minutes since 2021-01-01T00:30:00'

        # Expected output when transforming to output epoch is to add
        # 1 day (1440 minutes) to the input values
        values_with_output_epoch = np.add(dimension_values, 1440.0)

        with self.subTest('Non-temporal dimension.'):
            spatial_dimension = DimensionInformation('/variable',
                                                     dimension_values,
                                                     'degrees_east')
            assert_array_equal(spatial_dimension.get_values(), dimension_values)

        with self.subTest('Temporal dimension, no output epoch.'):
            temporal_dimension = DimensionInformation('/variable',
                                                      dimension_values,
                                                      input_temporal_units)
            assert_array_equal(temporal_dimension.get_values(), dimension_values)

        with self.subTest('Temporal dimension, output epoch.'):
            temporal_dimension = DimensionInformation('/variable',
                                                      dimension_values,
                                                      input_temporal_units)
            assert_array_equal(
                temporal_dimension.get_values(output_temporal_units),
                values_with_output_epoch
            )

    def test_netcdf4_dimension_information(self):
        """ Ensure a dimension variable will have the expected information
            extracted from the NetCDF-4 file, including path, values,
            units metadata and and temporal unit or epoch information. A non-
            temporal dimension should populate those last two items with `None`
            values.

        """
        with self.subTest('A non-temporal dimension is extracted'):
            spatial_dimension = NetCDF4DimensionInformation(self.test_dataset,
                                                            '/longitude')
            self.assertEqual(spatial_dimension.dimension_path, '/longitude')
            self.assertEqual(spatial_dimension.units, 'degrees_east')
            self.assertIsNone(spatial_dimension.epoch)
            self.assertIsNone(spatial_dimension.time_unit)
            assert_array_equal(spatial_dimension.values,
                               self.test_dataset['/longitude'][:])
            # bounds are not defined, so values and path should be `None`:
            self.assertIsNone(spatial_dimension.bounds_path)
            self.assertIsNone(spatial_dimension.bounds_values)

        with self.subTest('A temporal dimension is extracted'):
            time_dimension = NetCDF4DimensionInformation(self.test_dataset,
                                                         '/time')
            self.assertEqual(time_dimension.dimension_path, '/time')
            self.assertEqual(time_dimension.units, self.temporal_units)
            self.assertEqual(time_dimension.epoch, self.test_epoch)
            self.assertEqual(time_dimension.time_unit, timedelta(seconds=1))
            assert_array_equal(time_dimension.values,
                               self.test_dataset['/time'][:])

            # bounds are not defined, so values and path should be `None`:
            self.assertIsNone(time_dimension.bounds_path)
            self.assertIsNone(time_dimension.bounds_values)

        with self.subTest('A dimension with bounds information is extracted'):
            dimension_data = np.linspace(0, 5, 6)
            bounds_data = np.array([[-0.5, 0.5],
                                    [0.5, 1.5],
                                    [1.5, 2.5],
                                    [2.5, 3.5],
                                    [3.5, 4.5],
                                    [4.5, 5.5]])
            bounds_dataset = self.generate_netcdf_with_bounds('bounds.nc4',
                                                              'dimension',
                                                              dimension_data,
                                                              bounds_data)

            bounds_dimension = NetCDF4DimensionInformation(bounds_dataset,
                                                           '/dimension')

            self.assertEqual(bounds_dimension.dimension_path, '/dimension')
            self.assertEqual(bounds_dimension.units,
                             'seconds since 2001-01-01T00:00:00')
            self.assertEqual(bounds_dimension.epoch, datetime(2001, 1, 1))
            self.assertEqual(bounds_dimension.time_unit, timedelta(seconds=1))
            assert_array_equal(bounds_dimension.values, dimension_data)
            self.assertEqual(bounds_dimension.bounds_path, '/dimension_bnds')
            assert_array_equal(bounds_dimension.bounds_values, bounds_data)

            if bounds_dataset.isopen():
                bounds_dataset.close()

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
                self.temporal_units
            )

        with self.subTest('Attribute is present, ignores default value'):
            self.assertEqual(
                get_nc_attribute(
                    self.test_dataset['/time'], 'units', 'default'
                ),
                self.temporal_units
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

    def test_scale_to_integers(self):
        """ Ensure the input array is scaled such that all values are integers.
            These integers should equal the original values scaled by the
            returned scale factor.

        """
        with self.subTest('Floating input'):
            output_integers, scale_factor = scale_to_integers(
                np.array([0.0, 0.125, 0.25, 0.375, 0.5])
            )

            assert_array_equal(output_integers, np.array([0, 125, 250, 375, 500]))
            self.assertEqual(scale_factor, 1000)

        with self.subTest('Integer input'):
            input_values = np.array([1, 2, 3, 4])
            output_integers, scale_factor = scale_to_integers(input_values)

            assert_array_equal(output_integers, input_values)
            self.assertEqual(scale_factor, 1)

        with self.subTest('Recurring decimal in array, no infinite loop'):
            # The while loop has a cut-off at 1e-10
            output_integers, scale_factor = scale_to_integers(
                np.array([0.0, 0.99999999999, 2.0])
            )
            assert_array_equal(output_integers, np.array([0, 1e10, 2e10]))
            self.assertEqual(scale_factor, 1e10)

    def test_get_resolution(self):
        """ Ensure the correct resolution is calculated for input values. This
            function also needs to be able to handle the case of a single input
            value - where the resolution is calculated to be zero.

            GPM times are 2020-01-01 and 2020-01-02.

        """
        merra_times = np.concatenate([np.linspace(0, 1380, 24),
                                      np.linspace(2880, 4260, 24)])
        test_args = [
            ['MERRA-2 temporal', merra_times, 60.0],
            ['GPM/IMERG temporal', np.array([1577836800, 1577923200]), 86400],
            ['Spatial', np.array([0.25, 0.0, 0.625]), 0.125],
            ['Single input value', np.array([10.0]), 0.0]
        ]

        for description, input_values, expected_resolution in test_args:
            with self.subTest(description):
                self.assertEqual(get_resolution(input_values),
                                 expected_resolution)

    def test_get_grid_values(self):
        """ Ensure that a linearly spaced grid is returned. It should have a
            spacing corresponding to the supplied resolution and all input
            values should lie on a point in the grid.

        """
        with self.subTest('Multiple input values.'):
            input_values = np.array([0.25, 0.0, 0.625])
            grid_resolution = 0.125
            output_grid = get_grid_values(input_values, grid_resolution)

            # Ensure all points are separated by the specified resolution:
            self.assertTrue(all(np.diff(output_grid) == grid_resolution))

            # Ensure all values from the input are in the output:
            self.assertTrue(all(np.in1d(input_values, output_grid)))

            # Ensure grid doesn't extend beyond the full range of the input:
            self.assertEqual(output_grid.min(), input_values.min())
            self.assertEqual(output_grid.max(), input_values.max())

        with self.subTest('Single input values.'):
            input_values = np.array([0.25])
            output_grid = get_grid_values(input_values, 0.0)

            # For a single input value, the output should be same as input:
            assert_array_equal(input_values, output_grid)
