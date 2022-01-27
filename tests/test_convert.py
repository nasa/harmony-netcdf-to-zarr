""" Tests the Harmony convert module """
from unittest import TestCase
from pytest import raises

from harmony_netcdf_to_zarr.convert import compute_chunksize


class TestConvert(TestCase):
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
        chunksize_result = compute_chunksize(shape=(100, 100, 100), datatype='f8')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_medium(self):
        """
        Test of compute_chunksize method for a medium input shape
        """
        chunksize_expected = (100, 140, 140)
        chunksize_result = compute_chunksize(shape=(100, 1000, 1000), datatype='f8')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_large(self):
        """
        Test of compute_chunksize method for a large input shape
        """
        chunksize_expected = (125, 125, 125)
        chunksize_result = compute_chunksize(shape=(1000, 1000, 1000), datatype='f8')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_with_compression_args(self):
        """
        Test of compute_chunksize method with non-default compression args
        """
        chunksize_expected = (100, 680, 680)
        chunksize_result = compute_chunksize(shape=(100, 1000, 1000),
                                             datatype='i4',
                                             compression_ratio=6.8,
                                             compressed_chunksize_byte='26.8 Mi')
        self.assertTupleEqual(chunksize_expected, chunksize_result)

    def test_compute_chunksize_wrong_arguments(self):
        """
        Test of compute_chunksize method for a large input shape
        """
        with raises(ValueError) as execinfo:
            compute_chunksize(shape=(100, 1000, 1000),
                              datatype='i4',
                              compression_ratio=6.8,
                              compressed_chunksize_byte='26.8 MB')
        err_message_expected = """Chunksize needs to be either an integer or string.
If it's a string, assuming it follows NIST standard for binary prefix
    (https://physics.nist.gov/cuu/Units/binary.html)
except that only Ki, Mi, and Gi are allowed."""
        self.assertEqual(str(execinfo.value), err_message_expected)

    def tearDown(self):
        pass
