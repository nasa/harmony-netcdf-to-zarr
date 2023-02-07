# Test the rechunk functionality.

import numpy as np
from shutil import rmtree
from tempfile import mkdtemp, TemporaryDirectory
from unittest import TestCase
import xarray as xr
import zarr

from harmony_netcdf_to_zarr.rechunk import (_groups_from_zarr,
                                            get_target_chunks,
                                            rechunk_zarr)


class TestRechunk(TestCase):
    """Tests rechunking functions."""

    def setUp(self):
        self.tmp_dir = mkdtemp()

    def tearDown(self):
        rmtree(self.tmp_dir)

    def create_basic_store(self, location, groups=['']):
        """Creates a basic dataset for testing.

        Creates 4 varaibles [lon, lat, temperature, and precipitation],
        Sets lon and lat to be coordinate variables.
        It writes the dataset to a zarr store.

        Optionally, if the groups variable contains an array of values, each of
        these will be used as groups and the same 4 variables will also be
        written to the group.  This is only for testing nested zarr
        stores.

        """
        lon = np.arange(-180, 180, step=.1)
        lat = np.arange(-90, 90, step=.1)
        temperature = np.ones((3600, 1800), np.dtype('i2'))
        precipitation = np.ones((3600, 1800), np.dtype('float64'))
        ds = xr.Dataset(
            data_vars=dict(
                temperature=(["lon", "lat"], temperature),
                precipitation=(["lon", "lat"], precipitation),
            ),
            coords=dict(
                lon=(["lon"], lon),
                lat=(["lat"], lat),
            ),
            attrs=dict(description="sample dataset."),
        )
        for group in groups:
            ds.to_zarr(location, group=group, consolidated=True)

        zarr.consolidate_metadata(location)

    def test__groups_from_zarr_returns_root_group(self):
        """Test a store with no explicit groups or subgroups."""
        expected_groups = ['']
        with TemporaryDirectory() as store_location:
            self.create_basic_store(store_location)
            actual_groups = _groups_from_zarr(store_location)
            self.assertEqual(expected_groups, actual_groups)

    def test__groups_from_zarr_returns_nested_groups(self):
        """Test a store with explicit groups."""

        expected_groups = ['', 'Grid']
        with TemporaryDirectory() as store_location:
            self.create_basic_store(store_location, groups=['Grid'])
            actual_groups = _groups_from_zarr(store_location)
            self.assertEqual(expected_groups, actual_groups)

    def test__groups_from_zarr_returns_all_nested_groups(self):
        """Test a store with multiple groups and subgroups."""

        expected_groups = ['', 'Grid1', 'Grid2', 'Grid3', 'Grid3/sub']
        with TemporaryDirectory() as store_location:
            self.create_basic_store(store_location,
                                    groups=['Grid1', 'Grid2', 'Grid3/sub'])
            actual_groups = _groups_from_zarr(store_location)
            self.assertEqual(expected_groups, actual_groups)

    def test_get_target_chunks_root_dataset(self):
        """Test creating target chunks for a sample dataset."""

        expected_chunks = {
            '/precipitation': (1402, 1402),
            '/temperature': (3600, 1800),
            '/lat': None,
            '/lon': None
        }
        with TemporaryDirectory() as store_location:
            self.create_basic_store(store_location)
            actual_chunks = get_target_chunks(store_location)
            self.assertEqual(expected_chunks, actual_chunks)

    def test_get_target_chunks_grouped_dataset(self):
        """Test creating target chunks for a grouped sample dataset."""

        expected_chunks = {
            'Grid/precipitation': (1402, 1402),
            'Grid/temperature': (3600, 1800),
            'Grid/lat': None,
            'Grid/lon': None
        }
        with TemporaryDirectory() as store_location:
            self.create_basic_store(store_location, groups=['Grid'])
            actual_chunks = get_target_chunks(store_location)
            self.assertEqual(expected_chunks, actual_chunks)

    def test_get_target_chunks_deeply_grouped_dataset(self):
        """Test creating target chunks for a deeply grouped sample dataset."""

        expected_chunks = {
            'Grid1/precipitation': (1402, 1402),
            'Grid1/temperature': (3600, 1800),
            'Grid1/lat': None,
            'Grid1/lon': None,
            'Grid2/sub/precipitation': (1402, 1402),
            'Grid2/sub/temperature': (3600, 1800),
            'Grid2/sub/lat': None,
            'Grid2/sub/lon': None
        }
        with TemporaryDirectory() as store_location:
            self.create_basic_store(store_location,
                                    groups=['Grid1', 'Grid2/sub'])
            actual_chunks = get_target_chunks(store_location)
            self.assertEqual(expected_chunks, actual_chunks)

    def test_rechunking(self):
        """Test rechunking functionality"""
        with TemporaryDirectory() as store_location, \
             TemporaryDirectory() as tmp_location, \
             TemporaryDirectory() as target_location:

            self.create_basic_store(store_location)

            rechunk_zarr(store_location, target_location, tmp_location)
            target_zarr = zarr.open(target_location)
            actual_precipitation_chunks = target_zarr['precipitation'].chunks
            actual_temperature_chunks = target_zarr['temperature'].chunks
            self.assertEqual((3600, 1800), actual_temperature_chunks)
            self.assertEqual((1402, 1402), actual_precipitation_chunks)
