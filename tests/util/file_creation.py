#!/usr/bin/env python
from datetime import datetime
from os.path import join as path_join
from tempfile import mkdtemp, mkstemp
from typing import List
from uuid import NAMESPACE_URL, uuid4, uuid5

from harmony.util import bbox_to_geometry
from netCDF4 import Dataset
from pystac import Asset, Catalog, Item
import numpy as np

ROOT_METADATA_VALUES = dict(
    Conventions="CF-1.6",
    title="Test NetCDF 4",
    summary="Test NetCDF 4 file with small data and attributes like a swath",
    id="harmony_test_netcdf4",
    naming_authority="gov.nasa.earthdata.harmony",
    product_version=1,
    uuid=str(uuid5(NAMESPACE_URL, "harmony_test_netcdf4"))
)


def create_input_catalog(file_names: List[str]) -> str:
    """ A helper function to create a `pystac.Catalog` with an item for each
        input filename. These item will have a single asset, with the role of
        'data', and the supplied filename. The STAC catalog will then be saved
        to a local file, and the path to that catalog file will be returned.

    """
    catalog = Catalog('test input', 'description')

    for index, file_name in enumerate(file_names):
        item = Item(f'id{index}', bbox_to_geometry([-11.1, -22.2, 33.3, 44.4]),
                    [-11.1, -22.2, 33.3, 44.4], None,
                    {'start_datetime': '2020-01-01T00:00:00',
                     'end_datetime': '2020-01-02T00:00:00'})
        item.add_asset('data', Asset(f'file://{file_name}', roles=['data'],
                                     media_type='application/x-netcdf4'))
        catalog.add_item(item)

    catalog_directory = mkdtemp()
    catalog.normalize_and_save(catalog_directory)

    return path_join(catalog_directory, 'catalog.json')


def create_full_dataset(filename=None):
    filename = filename or path_join(mkdtemp(), f'{str(uuid4())}.nc4')

    with Dataset(filename, 'w') as ds:
        for k, v in ROOT_METADATA_VALUES.items():
            setattr(ds, k, v)

        ds.createDimension('ni', 3)
        ds.createDimension('nj', 3)
        ds.createDimension('time', 1)

        location_grp = ds.createGroup('location')
        location_grp.description = 'Group for dimension scales holding geolocation info'

        lats = location_grp.createVariable('lat', 'f4', ('ni', 'nj'), zlib=True)
        lats.standard_name = 'latitude'
        lats.long_name = 'latitude'
        lats.units = 'degrees_north'
        lats.valid_min = -90.0
        lats.valid_max = 90.0

        lons = location_grp.createVariable('lon', 'f4', ('ni', 'nj'), zlib=True)
        lons.standard_name = 'longitude'
        lons.long_name = 'longitude'
        lons.units = 'degrees_east'
        lons.valid_min = -180.0
        lons.valid_max = 180.0

        times = ds.createVariable('time', 'i4', ('time', ), zlib=True)
        times.units = "hours since 2001-01-01 00:00:00.0"
        times.calendar = "gregorian"

        data_grp = ds.createGroup('data')
        data_grp.description = 'Group to hold the data'

        ns_grp = data_grp.createGroup('vertical')
        ns_grp.description = 'Group to hold North and South pointing data'

        ew_grp = data_grp.createGroup('horizontal')
        ew_grp.description = 'Group to hold East and West pointing data'

        n_var = ns_grp.createVariable('north', 'u1', ('time', 'ni', 'nj'),
                                      zlib=True, fill_value=127)
        n_var.coordinates = 'lon lat'
        w_var = ew_grp.createVariable('west', 'u1', ('time', 'ni', 'nj'),
                                      zlib=True, fill_value=127)
        w_var.coordinates = 'lon lat'
        s_var = ns_grp.createVariable('south', 'u1', ('time', 'ni', 'nj'),
                                      zlib=True, fill_value=127)
        s_var.coordinates = 'lon lat'
        e_var = ew_grp.createVariable('east', 'u1', ('time', 'ni', 'nj'),
                                      zlib=True, fill_value=127)
        e_var.coordinates = 'lon lat'
        e_var.scale_factor = 2
        e_var.valid_range = [0, 25]
        e_var.valid_min = 0
        e_var.valid_max = 25

        # Define the data as a tilted square.  Tilt your head 45 degrees to the
        # right to see it.
        lats[:, :] = [
            [0.0, 5.5, 11.0],
            [-5.5, 0.0, 5.5],
            [-11.0, -5.5, 0.0]
        ]
        lons[:, :] = np.rot90(lats, 3, axes=(0, 1))

        times[0] = 166536  # January 1st 2020

        # Data consists of values whose maximum points at one corner of the
        # square.  The North variable's maximum is at the northernmost corner,
        # West at the westernmost, etc.
        n_var[0, :, :] = [
            [4, 8, 16],
            [0, 4, 8],
            [0, 0, 4]
        ]
        w_var[0, :, :] = np.rot90(n_var[0], 1, axes=(0, 1))
        s_var[0, :, :] = np.rot90(n_var[0], 2, axes=(0, 1))
        e_var[0, :, :] = np.rot90(n_var[0], 3, axes=(0, 1))

    return filename


def create_large_dataset(filename=None):
    filename = filename or mkstemp(suffix='.nc4')[1]
    with Dataset(filename, 'w') as ds:
        num_points = 10000

        ds.createDimension('dummy_dim', num_points)

        dummy_dim = ds.createVariable('dummy_dim', 'i4', ('dummy_dim', ),
                                      zlib=True)

        data_grp = ds.createGroup('data')
        data_grp.description = 'Group to hold the data'

        data_var = data_grp.createVariable('var', 'i4', ('dummy_dim', ),
                                           zlib=True, fill_value=127,
                                           chunksizes=(365,))

        # Fill dimension values
        dummy_dim[:] = np.arange(num_points)

        # Fill data values
        data_var[:] = np.arange(num_points)

    return filename


def create_gpm_dataset(test_dir: str, file_datetime: datetime) -> str:
    """ Create a granule representative of GPM data. """
    epoch_datetime = datetime(1970, 1, 1)
    filename = path_join(test_dir, f'{uuid4()}.nc4')

    science_variables = ['HQObservationTime', 'HQPrecipitation',
                         'HQPrecipSource', 'IRkalmanFilterWeight',
                         'IRPrecipitation', 'precipitationCal',
                         'precipitationQualityIndex', 'precipitationUncal',
                         'probabilityLiquidPrecipitation', 'randomError']

    with Dataset(filename, 'w') as dataset:
        grid_group = dataset.createGroup('Grid')
        grid_group.createDimension('lat', 1800)
        lat = grid_group.createVariable('lat', np.float32, ('lat',))
        lat[:] = np.linspace(-89.95, 89.95, 1800, dtype=np.float32)
        lat.setncattr('units', 'degrees_north')
        lat.setncattr('bounds', 'lat_bnds')
        lat.setncattr('standard_name', 'latitude')
        lat.setncattr('axis', 'Y')

        grid_group.createDimension('latv', 2)
        lat_bounds = grid_group.createVariable('lat_bnds', np.float32,
                                               ('lat', 'latv'))
        lat_bounds_values = np.array([
            np.linspace(-90.0, 89.9, 1800, dtype=np.float32),
            np.linspace(-89.9, 90.0, 1800, dtype=np.float32)
        ], dtype=np.float32)

        lat_bounds[:] = lat_bounds_values.T
        lat_bounds.setncattr('units', 'degrees_north')

        grid_group.createDimension('lon', 3600)
        lon = grid_group.createVariable('lon', np.float32, ('lon',))
        lon[:] = np.linspace(-179.95, 179.95, 3600, dtype=np.float32)
        lon.setncattr('units', 'degrees_east')
        lon.setncattr('bounds', 'lon_bnds')
        lon.setncattr('standard_name', 'longitude')
        lon.setncattr('axis', 'X')

        grid_group.createDimension('lonv', 2)
        lon_bounds = grid_group.createVariable('lon_bnds', np.float32,
                                               ('lon', 'lonv'))

        lon_bounds_values = np.array([
            np.linspace(-180.0, 179.9, 3600, dtype=np.float32),
            np.linspace(-179.9, 180.0, 3600, dtype=np.float32)
        ], dtype=np.float32)

        lon_bounds[:] = lon_bounds_values.T
        lon_bounds.setncattr('units', 'degrees_east')

        grid_group.createDimension('time', 1)
        time_variable = grid_group.createVariable('time', np.float64,
                                                  ('time',))
        time_variable[0] = (file_datetime - epoch_datetime).total_seconds()
        time_variable.setncattr('units', 'seconds since 1970-01-01T00:00:00')
        time_variable.setncattr('bounds', 'time_bnds')
        time_variable.setncattr('standard_name', 'time')
        time_variable.setncattr('calendar', 'julian')
        time_variable.setncattr('axis', 'T')

        grid_group.createDimension('nv', 2)
        time_bounds = grid_group.createVariable('time_bnds', np.float64,
                                                ('time', 'nv'))
        time_bounds[0][0] = time_variable[0]
        time_bounds[0][1] = time_variable[0] + 1800.0
        time_bounds.setncattr('units', 'seconds since 1970-01-01T00:00:00')

        for variable_name in science_variables:
            variable = grid_group.createVariable(variable_name, np.float64,
                                                 ('time', 'lon', 'lat'),
                                                 fill_value=-9999)
            variable[:] = np.random.rand(1, 3600, 1800)
            variable.setncattr('coordinates', 'time lon lat')
            variable.setncattr('DimensionNames', 'time,lon,lat')

    return filename
