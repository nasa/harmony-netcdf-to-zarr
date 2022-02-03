"""
==================
adapter.py
==================

Service adapter for converting NetCDF4 to Zarr
"""

from os import environ
import shutil
from tempfile import mkdtemp

from pystac import Asset

import harmony
from harmony.util import generate_output_filename, download, HarmonyException
from .convert import netcdf_to_zarr, make_localstack_s3fs, make_s3fs


class ZarrException(HarmonyException):
    """
    Exception thrown during Zarr conversion
    """

    def __init__(self, message=None):
        super().__init__(message, 'harmonyservices/netcdf-to-zarr')


class NetCDFToZarrAdapter(harmony.BaseHarmonyAdapter):
    """
    Translates NetCDF4 to Zarr
    """

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
        """
        Downloads, translates to Zarr, then re-uploads granules
        """
        if (
            not self.message.format
            or not self.message.format.mime
            or self.message.format.mime not in ['application/zarr', 'application/x-zarr']
        ):
            self.logger.error(f'The Zarr formatter cannot convert to {self.message.format}, skipping')
            raise ZarrException('Request failed due to an incorrect service workflow')
        self.message.format.process('mime')
        return super().invoke()

    def process_item(self, item, source):
        """
        Converts an input STAC Item's data into Zarr, returning an output STAC item

        Parameters
        ----------
        item : pystac.Item
            the item that should be converted
        source : harmony.message.Source
            the input source defining the variables, if any, to subset from the item

        Returns
        -------
        pystac.Item
            a STAC item containing the Zarr output
        """
        result = item.clone()
        result.assets = {}

        # Create a temporary dir for processing we may do
        workdir = mkdtemp()
        try:
            # Get the data file
            asset = next(v for k, v in item.assets.items() if 'data' in (v.roles or []))
            input_filename = download(
                asset.href,
                workdir,
                logger=self.logger,
                access_token=self.message.accessToken,
                cfg=self.config
            )

            name = generate_output_filename(asset.href, ext='.zarr')
            root = self.message.stagingLocation + name

            try:
                store = self.s3.get_mapper(root=root, check=False, create=True)
                netcdf_to_zarr(input_filename, store)
            except Exception as e:
                # Print the real error and convert to user-facing error that's more digestible
                self.logger.error(e, exc_info=1)
                filename = asset.href.split('?')[0].rstrip('/').split('/')[-1]
                raise ZarrException(f'Could not convert file to Zarr: {filename}') from e

            # Update the STAC record
            result.assets['data'] = Asset(root, title=name, media_type='application/x-zarr', roles=['data'])

            # Return the STAC record
            return result
        finally:
            # Clean up any intermediate resources
            shutil.rmtree(workdir)
