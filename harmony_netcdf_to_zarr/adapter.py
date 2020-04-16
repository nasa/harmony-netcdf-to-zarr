"""
==================
adapter.py
==================

Service adapter for converting NetCDF4 to Zarr
"""

from os import environ
import harmony
import zarr
import s3fs
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
        logger = self.logger

        granules = self.message.granules

        result = None
        for i, granule in enumerate(granules):
            try:
                self.download_granules([granule])
                name = self.filename_for_granule(granule, '.zarr')
                root = self.message.stagingLocation + name
                store = self.s3.get_mapper(root=root, check=False, create=True)
                netcdf_to_zarr(granule.local_filename, store)

                progress = int(100 * (i + 1) / len(granules))
                self.async_add_url_partial_result(root, title=name, mime='application/x-zarr', progress=progress, source_granule=granule)
            except:
                self.completed_with_error('Could not convert granule to Zarr: ' + granule.id)
                raise # We could opt to continue when things are known-stable.  For now, avoid downloading just to fail repeatedly
            finally:
                # Clean up after each granule to avoid disk space issues
                self.cleanup()

        # TODO: (HARMONY-150) If there are multiple granules and the result isn't async ... then what?
        self.async_completed_successfully()
