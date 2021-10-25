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

    def test_suggest_chunksize(self):
        """
        Test for suggest_chunksize method
        """
        #pytest.set_trace()
        chunksize_expected = (211, 211, 211)
        chunksize_result = convert.suggest_chunksize(shape=(10000, 1000,1000), datatype='f8')
        assert chunksize_expected == chunksize_result

    def tearDown(self):
        pass
