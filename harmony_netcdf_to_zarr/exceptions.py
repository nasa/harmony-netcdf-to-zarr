""" This module contains custom exceptions specific to the NetCDF-to-Zarr
    service. These exceptions are intended to allow for easier debugging of the
    expected errors that may occur during an invocation of the service.

"""


class CustomError(Exception):
    """ Base class for exceptions in the NetCDF-to-Zarr service. This base
        class could be extended in the future to assign exit codes, for
        example.

    """
    def __init__(self, exception_type, message):
        self.exception_type = exception_type
        self.message = message
        super().__init__(self.message)


class MixedDimensionTypeError(CustomError):
    """ This exception is raised when a dimension variable present in all input
        granules has some with `units` metadata attributes that indicate a
        temporal dimension and others that indicate a non-temporal dimension.

    """
    def __init__(self, dimension_variable):
        super().__init__('MixedDimensionTypeError',
                         (f'{dimension_variable} has mixed input types, both '
                          'temporal and non-temporal.'))
