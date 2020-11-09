"""
==================
adapter.py
==================

Service adapter for converting NetCDF4 to Zarr
"""

from os import environ
import shutil
from tempfile import mkdtemp

import s3fs
from pystac import Asset

import harmony
from harmony.util import generate_output_filename, download, HarmonyException
from .convert import netcdf_to_zarr

region = environ.get('AWS_DEFAULT_REGION') or 'us-west-2'


def make_localstack_s3fs():
    host = environ.get('LOCALSTACK_HOST') or 'host.docker.internal'
    return s3fs.S3FileSystem(
        use_ssl=False,
        key='ACCESS_KEY',
        secret='SECRET_KEY',
        client_kwargs=dict(
            region_name=region,
            endpoint_url='http://%s:4572' % (host)))


def make_s3fs():
    return s3fs.S3FileSystem(client_kwargs=dict(region_name=region))


class ZarrException(HarmonyException):
    """
    Exception thrown during Zarr conversion
    """

    def __init__(self, message=None):
        super().__init__(message, 'harmony/netcdf-to-zarr')


class NetCDFToZarrAdapter(harmony.BaseHarmonyAdapter):
    """
    Translates NetCDF4 to Zarr
    """

    def __init__(self, message):
        super().__init__(message)

        if environ.get('USE_LOCALSTACK') == 'true':
            self.s3 = make_localstack_s3fs()
        else:
            self.s3 = make_s3fs()

    def invoke(self):
        """
        Downloads, translates to Zarr, then re-uploads granules
        """
        format = self.message.format
        if format and format.mime and format.mime in ['application/zarr', 'application/x-zarr']:
            format.process('mime')
            return super().invoke()
        self.logger.warn('The zarr formatter cannot convert to %s, skipping' % (format.mime,))
        return self.catalog

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
            input_filename = download(asset.href, workdir, logger=self.logger, access_token=self.message.accessToken)

            name = generate_output_filename(asset.href, ext='.zarr')
            root = self.message.stagingLocation + name

            try:
                store = self.s3.get_mapper(root=root, check=False, create=True)
                netcdf_to_zarr(input_filename, store)
            except Exception as e:
                # Print the real error and convert to user-facing error that's more digestible
                self.logger.error(e, exc_info=1)
                filename = asset.href.split('?')[0].rstrip('/').split('/')[-1]
                raise ZarrException('Could not convert file to Zarr: %s' % (filename))

            # Update the STAC record
            result.assets['data'] = Asset(root, title=name, media_type='text/plain', roles=['data'])

            # Return the STAC record
            return result
        finally:
            # Clean up any intermediate resources
            shutil.rmtree(workdir)
