"""
==================
adapter.py
==================

Service adapter for converting NetCDF4 to Zarr
"""
from os import environ
from os.path import join as path_join
from shutil import rmtree
from tempfile import mkdtemp

from harmony import BaseHarmonyAdapter
from harmony.util import generate_output_filename, HarmonyException

from .convert import make_localstack_s3fs, make_s3fs, mosaic_to_zarr
from .download_utilities import download_granules
from .stac_utilities import get_netcdf_urls, get_output_catalog


ZARR_MEDIA_TYPES = ['application/zarr', 'application/x-zarr']


class ZarrException(HarmonyException):
    """ Exception thrown during Zarr conversion """

    def __init__(self, message=None):
        super().__init__(message, 'harmonyservices/netcdf-to-zarr')


class NetCDFToZarrAdapter(BaseHarmonyAdapter):
    """ Translates NetCDF4 to Zarr """

    def __init__(self, message, catalog=None, config=None):
        """
        Constructs the adapter

        Parameters
        ----------
        message : harmony.Message
            The Harmony input which needs acting upon
        catalog : pystac.Catalog
            A STAC catalog containing the files on which to act
        config : harmony.util.Config
            The configuration values for this runtime environment.
        """
        super().__init__(message, catalog=catalog, config=config)

        if environ.get('USE_LOCALSTACK') == 'true':
            self.s3 = make_localstack_s3fs()
        else:
            self.s3 = make_s3fs()

    def invoke(self):
        """ Downloads, translates to Zarr, then re-uploads granules. The
            `invoke` class method also validates the request by ensuring that
            the requested output format is Zarr, and a STAC catalog is provided
            to the service.

        """
        if (
            not self.message.format
            or not self.message.format.mime
            or self.message.format.mime not in ZARR_MEDIA_TYPES
        ):
            self.logger.error('The Zarr formatter cannot convert to '
                              f'{self.message.format}, skipping')
            raise ZarrException('Request failed due to an incorrect service '
                                'workflow')
        elif not self.catalog:
            raise ZarrException('Invoking NetCDF-to-Zarr without STAC catalog '
                                'is not supported.')
        else:
            self.message.format.process('mime')
            return (self.message, self.process_items_many_to_one())

    def process_items_many_to_one(self):
        """ Converts an input STAC Item's data into Zarr, returning an output
            STAC catalog. This is a many-to-one operation by default. For
            one-to-one operations, it is assumed that the `concatenate` query
            parameter is False, and Harmony will invoke this backend service
            once per input granule. Because of this, each backend invocation is
            expected to produce a single Zarr output.

        """
        workdir = mkdtemp()
        try:
            items = list(self.get_all_catalog_items(self.catalog))
            netcdf_urls = get_netcdf_urls(items)

            local_file_paths = download_granules(netcdf_urls, workdir,
                                                 self.message.accessToken,
                                                 self.config, self.logger)

            if len(local_file_paths) == 1:
                output_name = generate_output_filename(netcdf_urls[0],
                                                       ext='.zarr')
            else:
                # Mimicking PO.DAAC Concise: for many-to-one the file name is
                # "<collection>_merged.zarr".
                collection = self._get_item_source(items[0]).collection
                output_name = f'{collection}_merged.zarr'

            zarr_root = path_join(self.message.stagingLocation, output_name)
            zarr_store = self.s3.get_mapper(root=zarr_root, check=False,
                                            create=True)

            mosaic_to_zarr(local_file_paths, zarr_store, logger=self.logger)

            return get_output_catalog(self.catalog, zarr_root)
        except Exception as service_exception:
            self.logger.error(service_exception, exc_info=1)
            raise ZarrException('Could not create Zarr output: '
                                f'{str(service_exception)}') from service_exception
        finally:
            rmtree(workdir)
