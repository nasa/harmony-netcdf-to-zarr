"""
Tests the Harmony convert module
"""

import unittest
import pytest

from harmony_netcdf_to_zarr import convert


class TestConvert(unittest.TestCase):
    """
    Tests the Harmony adapter
    """
    def setUp(self):
        pass

    def test_regenerate_chunks_small(self):
        """
        Test of regenerate_chunks method for a small input shape
        """
        chunksize_expected = (100, 100, 100)
        chunksize_result = convert.regenerate_chunks(shape=(100, 100,100), datatype='f8')
        assert chunksize_expected == chunksize_result

    def test_regenerate_chunks_medium(self):
        """
        Test of regenerate_chunks method for a medium input shape
        """
        chunksize_expected = (100, 307, 307)
        chunksize_result = convert.regenerate_chunks(shape=(100, 1000,1000), datatype='f8')
        assert chunksize_expected == chunksize_result

    def test_regenerate_chunks_large(self):
        """
        Test of regenerate_chunks method for a large input shape
        """
        chunksize_expected = (211, 211, 211)
        chunksize_result = convert.regenerate_chunks(shape=(1000, 1000,1000), datatype='f8')
        assert chunksize_expected == chunksize_result

    def tearDown(self):
        pass
