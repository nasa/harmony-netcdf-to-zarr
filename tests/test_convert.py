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

    def test_compute_chunksize_small(self):
        """
        Test of compute_chunksize method for a small input shape
        """
        chunksize_expected = (100, 100, 100)
        chunksize_result = convert.compute_chunksize(shape=(100, 100,100), datatype='f8')
        assert chunksize_expected == chunksize_result

    def test_compute_chunksize_medium(self):
        """
        Test of compute_chunksize method for a medium input shape
        """
        chunksize_expected = (100, 971, 971)
        chunksize_result = convert.compute_chunksize(shape=(100, 1000,1000), datatype='f8')
        assert chunksize_expected == chunksize_result

    def test_compute_chunksize_large(self):
        """
        Test of compute_chunksize method for a large input shape
        """
        chunksize_expected = (455, 455, 455)
        chunksize_result = convert.compute_chunksize(shape=(1000, 1000,1000), datatype='f8')
        assert chunksize_expected == chunksize_result

    def tearDown(self):
        pass
