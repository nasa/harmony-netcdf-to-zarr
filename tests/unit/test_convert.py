""" Tests the Harmony convert module """
from datetime import datetime
from itertools import chain, repeat
from logging import getLogger
from multiprocessing import Process
from os.path import join as path_join
from pytest import raises
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase
from unittest.mock import patch, Mock

from netCDF4 import Dataset
from numpy.testing import assert_array_equal
from zarr import (DirectoryStore, group as create_zarr_group,
                  ProcessSynchronizer)
import numpy as np

from harmony_netcdf_to_zarr.convert import (__copy_attrs as copy_attrs,
                                            __copy_group as copy_group,
                                            compute_chunksize,
                                            __get_aggregated_shape as get_aggregated_shape,
                                            __insert_data_slice as insert_data_slice,
                                            mosaic_to_zarr)
from harmony_netcdf_to_zarr.mosaic_utilities import DimensionsMapping
from tests.util.file_creation import create_gpm_dataset


class TestConvert(TestCase):
    """ Tests the functions in `harmony_netcdf_to_zarr.convert.py`. """
    def setUp(self):
        self.temp_dir = mkdtemp()

    def tearDown(self):
        rmtree(self.temp_dir)

    def test_compute_chunksize_small(self):
        """ Test of compute_chunksize method for a small input shape """
        chunksize_expected = (100, 100, 100)
        chunksize_result = compute_chunksize(shape=(100, 100, 100), datatype='f8')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_medium(self):
        """ Test of compute_chunksize method for a medium input shape """
        chunksize_expected = (100, 140, 140)
        chunksize_result = compute_chunksize(shape=(100, 1000, 1000), datatype='f8')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_large(self):
        """ Test of compute_chunksize method for a large input shape """
        chunksize_expected = (125, 125, 125)
        chunksize_result = compute_chunksize(shape=(1000, 1000, 1000), datatype='f8')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_with_compression_args(self):
        """ Test of compute_chunksize method with non-default compression
            args

        """
        chunksize_expected = (100, 680, 680)
        chunksize_result = compute_chunksize(shape=(100, 1000, 1000),
                                             datatype='i4',
                                             compression_ratio=6.8,
                                             compressed_chunksize_byte='26.8 Mi')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_wrong_arguments(self):
        """ Test of compute_chunksize method for a large input shape """
        with raises(ValueError) as execinfo:
            compute_chunksize(shape=(100, 1000, 1000),
                              datatype='i4',
                              compression_ratio=6.8,
                              compressed_chunksize_byte='26.8 MB')
        err_message_expected = (
            'Chunksize needs to be either an integer or string. If it\'s a '
            'string, assuming it follows NIST standard for binary prefix '
            '(https://physics.nist.gov/cuu/Units/binary.html) except that '
            'only Ki, Mi, and Gi are allowed.'
        )
        self.assertEqual(str(execinfo.value), err_message_expected)

    def test_copy_attrs(self):
        """ Ensure that attributes are copied to a Zarr store, and that any
            pre-existing attributes are not removed or overwritten (either
            by values from the NetCDF-4 attributes or keyword arguments to the
            function).

            Attributes included as keyword arguments to the `__copy_attrs`
            function should also be written to the Zarr store.

        """
        with self.subTest('Attributes only updated.'):
            zarr_store = DirectoryStore(path_join(self.temp_dir, 'test.zarr'))
            zarr_group = create_zarr_group(zarr_store)
            zarr_group.attrs.put({'attr_one': 'val_one', 'attr_two': 'val_two'})

            with Dataset('test.nc4', diskless=True, mode='w') as dataset:
                dataset.setncattr('attr_three', 'val_three')
                dataset.setncattr('attr_two', 'val_four_not_copied')

                copy_attrs(dataset, zarr_group, kwarg_attr=1.0,
                           attr_one='value_five_not_copied')

            self.assertDictEqual(zarr_group.attrs.asdict(),
                                 {'attr_one': 'val_one',
                                  'attr_two': 'val_two',
                                  'attr_three': 'val_three',
                                  'kwarg_attr': 1.0})

        with self.subTest('scaled_factor and add_offset written.'):
            zarr_store = DirectoryStore(path_join(self.temp_dir, 'test_two.zarr'))
            zarr_group = create_zarr_group(zarr_store)

            with Dataset('test.nc4', diskless=True, mode='w') as dataset:
                dataset.setncattr('scale_factor', 1.0)
                dataset.setncattr('add_offset', 0.0)
                dataset.setncattr('units', 'm')

                copy_attrs(dataset, zarr_group)

            self.assertDictEqual(zarr_group.attrs.asdict(),
                                 {'add_offset': 0.0, 'scale_factor': 1.0,
                                  'units': 'm'})

        with self.subTest('kwargs take priority over NetCDF-4 metadata'):
            zarr_store = DirectoryStore(path_join(self.temp_dir, 'test_kwarg.zarr'))
            zarr_group = create_zarr_group(zarr_store)

            with Dataset('test.nc4', diskless=True, mode='w') as dataset:
                dataset.setncattr('units', 'NetCDF-4 units')

                copy_attrs(dataset, zarr_group, units='kwarg units')

            self.assertDictEqual(zarr_group.attrs.asdict(),
                                 {'units': 'kwarg units'})

    def test_get_aggregated_shape(self):
        """ Ensure that correct shape is retrieved in each of the four
            possible situations:

            * An aggregated dimension should return new 1-D shape.
            * An aggregated bounds variable should return new 2-D shape.
            * A variable with an aggregated dimension should return new shape.
            * A variable with no aggregated dimensions should return old shape.

        """
        local_file_one = create_gpm_dataset(self.temp_dir,
                                            datetime(2021, 2, 28, 3, 30))

        local_file_two = create_gpm_dataset(self.temp_dir,
                                            datetime(2021, 2, 28, 4, 00))
        dim_mapping = DimensionsMapping([local_file_one, local_file_two])
        aggregated_dimensions = ['/Grid/time', '/Grid/time_bnds']

        with Dataset(local_file_one, 'r') as dataset:
            with self.subTest('Aggregated dimension'):
                self.assertTupleEqual(
                    get_aggregated_shape(dataset['/Grid/time'], dim_mapping,
                                         aggregated_dimensions),
                    (2, )
                )

            with self.subTest('Aggregated bounds variable'):
                self.assertTupleEqual(
                    get_aggregated_shape(dataset['/Grid/time_bnds'],
                                         dim_mapping, aggregated_dimensions),
                    (2, 2)
                )

            with self.subTest('Science variable with aggregated dimension'):
                self.assertTupleEqual(
                    get_aggregated_shape(dataset['/Grid/precipitationCal'],
                                         dim_mapping, aggregated_dimensions),
                    (2, 3600, 1800)
                )

            with self.subTest('Variable with non aggregated dimension'):
                self.assertTupleEqual(
                    get_aggregated_shape(dataset['/Grid/lon'],
                                         dim_mapping, aggregated_dimensions),
                    dataset['/Grid/lon'].shape
                )

    def test_insert_dataset_slice(self):
        """ Ensure that an input granule is inserted into the correct region
            of the output Zarr store dataset.

        """
        local_file_one = create_gpm_dataset(self.temp_dir,
                                            datetime(2021, 2, 28, 3, 30))

        local_file_two = create_gpm_dataset(self.temp_dir,
                                            datetime(2021, 2, 28, 4, 00))
        dim_mapping = DimensionsMapping([local_file_one, local_file_two])
        output_shape = (2, 3600, 1800)
        output_chunks = compute_chunksize(output_shape, np.float32)

        zarr_store = DirectoryStore(path_join(self.temp_dir, 'test.zarr'))
        zarr_group = create_zarr_group(zarr_store)
        zarr_variable = zarr_group.create_dataset('precipitationCal',
                                                  shape=output_shape,
                                                  chunks=output_chunks,
                                                  dtype=np.float64,
                                                  fill_value=-9999.0)

        with Dataset(local_file_one, 'r') as dataset:
            insert_data_slice(dataset['/Grid/precipitationCal'], zarr_variable,
                              '/Grid/precipitationCal', dim_mapping)

            assert_array_equal(zarr_variable[0][:],
                               dataset['/Grid/precipitationCal'][0])

        # Second time slice in the lon/lat plane should still be all fill values:
        assert_array_equal(zarr_variable[1][:], np.ones((3600, 1800)) * -9999.0)

    @patch('harmony_netcdf_to_zarr.convert.__copy_variable')
    def test_copy_group(self, mock_copy_variable):
        """ Ensure that the copy_group function recurses to the point where
            the `__copy_variable` function is called for each variable.

        """
        test_granule = create_gpm_dataset(self.temp_dir,
                                          datetime(2021, 2, 28, 3, 30))

        dim_mapping = DimensionsMapping([test_granule])
        aggregated_dims = ['/Grid/time', '/Grid/time_bnds']
        zarr_store = DirectoryStore(path_join(self.temp_dir, 'test.zarr'))
        zarr_synchronizer = ProcessSynchronizer(path_join(self.temp_dir,
                                                          'test.sync'))
        zarr_group = create_zarr_group(zarr_store,
                                       synchronizer=zarr_synchronizer)

        with Dataset(test_granule, 'r') as dataset:
            copy_group(dataset, zarr_group, dim_mapping, aggregated_dims)
            all_input_variables = set(dataset['/Grid'].variables.keys())

        # There are 3 dimension variables, 3 bounds variables and 10 other
        # gridded variables.
        self.assertEqual(mock_copy_variable.call_count, 16)

        # The third argument in the call to `__copy_variable` is the variable
        # name.
        all_output_variables = {call[0][2]
                                for call
                                in mock_copy_variable.call_args_list}

        self.assertSetEqual(all_input_variables, all_output_variables)

    @patch('harmony_netcdf_to_zarr.convert.granule_chunk_shapes')
    @patch('harmony_netcdf_to_zarr.convert.Process')
    def test_failed_multiprocess(self, mock_process, mock_chunks):
        """Ensure killed subprocess propagates to parent.
        """
        logger = getLogger('test')

        test_granule = create_gpm_dataset(self.temp_dir,
                                          datetime(2021, 2, 28, 3, 30))

        mock_chunks.returns = {'unusedShapes': ()}
        zarr_store = DirectoryStore(path_join(self.temp_dir, 'test.zarr'))

        # Set up process.is_alive return values
        n_successfull_polls = 5
        effect = chain(repeat(True, n_successfull_polls), repeat(False))

        def side_effect():
            return next(effect)

        processes = [Mock(Process), Mock(Process)]
        for p in processes:
            p.exitcode = 0
            p.is_alive.side_effect = side_effect
        processes[0].exitcode = -9
        mock_process.side_effect = processes

        input_granules = [test_granule, test_granule, test_granule]

        regex_message = 'Problem writing data to Zarr store: processes exit codes: \[-9, 0.*'
        with self.assertRaisesRegex(RuntimeError, regex_message):
            mosaic_to_zarr(input_granules,
                           zarr_store=zarr_store,
                           process_count=2,
                           logger=logger)
