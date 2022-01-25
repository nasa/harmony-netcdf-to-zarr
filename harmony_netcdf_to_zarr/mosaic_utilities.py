""" A module including functions to take many input NetCDF-4 datasets and
    create aggregated dimensions, allowing for a single Zarr output.

"""
from datetime import timedelta
from typing import List, Optional, Union

from dateutil.parser import parse as parse_datetime

from netCDF4 import Dataset, Group, Variable
import numpy as np


NetCDF4Attribute = Union[bytes, str, np.integer, np.floating, np.ndarray]

seconds_delta = timedelta(seconds=1)
minutes_delta = timedelta(minutes=1)
hours_delta = timedelta(hours=1)
days_delta = timedelta(days=1)

time_delta_to_unit_map = {seconds_delta: 'seconds',
                          minutes_delta: 'minutes',
                          hours_delta: 'hours',
                          days_delta: 'days'}

time_unit_to_delta_map = {'seconds': seconds_delta,
                          'second': seconds_delta,
                          'secs': seconds_delta,
                          'sec': seconds_delta,
                          's': seconds_delta,
                          'minutes': minutes_delta,
                          'minute': minutes_delta,
                          'mins': minutes_delta,
                          'min': minutes_delta,
                          'hours': hours_delta,
                          'hour': hours_delta,
                          'hrs': hours_delta,
                          'hr': hours_delta,
                          'h': hours_delta,
                          'days': days_delta,
                          'day': days_delta,
                          'd': days_delta}


class DimensionsMapping:
    """ A class containing the information for all dimensions contained in each
        of the input NetCDF-4 granules. This class also will produce single,
        aggregated arrays and metadata (if required) for the output Zarr
        object.

    """
    def __init__(self, input_paths: List[str]):
        self.input_paths = input_paths
        self.input_dimensions = {}
        self.output_dimensions = {}  # DAS-1375
        self._map_input_dimensions()

    def _map_input_dimensions(self):
        """ Iterate through all input files and extract their dimension
            information.

        """
        for input_path in self.input_paths:
            with Dataset(input_path, 'r') as input_dataset:
                self._parse_group(input_dataset, input_dataset)

    def _parse_group(self, group: Union[Dataset, Group], dataset: Dataset):
        """ Iterate through group variables extracting each. Then recursively
            call this function to parse any subgroups.

        """
        for variable in group.variables.values():
            self._parse_variable_dimensions(variable, dataset)

        for nested_group in group.groups.values():
            self._parse_group(nested_group, dataset)

    def _parse_variable_dimensions(self, variable: Variable, dataset: Dataset):
        """ Extract the dimensions associated with a given variable.
            This function will only save those dimensions that have associated
            variables also within the input NetCDF-4 file.

        """
        for dimension_name in variable.dimensions:
            dimension_path = resolve_reference_path(variable, dimension_name)
            input_path = dataset.filepath()

            if is_variable_in_dataset(dimension_path, dataset):
                dim_data = self.input_dimensions.setdefault(dimension_path, {})

                if input_path not in dim_data:
                    dim_data[input_path] = DimensionInformation(dataset,
                                                                dimension_path)


class DimensionInformation:
    """ A class containing information on a dimension, including the path of
        the dimension variable within a NetCDF-4 or Zarr Dataset, the values of
        the 1-dimensional dimension array and any associated temporal epoch
        and unit. For TRT-121, it is initially assumed that non-temporal
        dimensions can be aggregated without requiring offsets similar to
        that stored in the temporal dimensions `units` metdata attribute.

        This class can be used to contain both the dimension information from
        individual input NetCDF-4 files, as well as the aggregated output
        dimension.

    """
    def __init__(self, dataset: Dataset, dimension_path: str):
        self.dimension_path = dimension_path
        self.values = dataset[dimension_path][:]
        self.units = get_nc_attribute(dataset[dimension_path], 'units')
        self.epoch = None
        self.time_unit = None
        self._get_epoch_and_unit()

    def _get_epoch_and_unit(self):
        """ Check the `units` attribute in the dimension variable metadata. If
            present, compare the format to the CF-Convention format for a
            temporal dimension (e.g., "seconds since 2000-01-02T03:04:05"), and
            extract the type of unit.

            For documentation on temporal dimensions see:

            https://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate

        """
        if self.units is not None and ' since ' in self.units:
            time_unit_string, epoch_string = self.units.split(' since ')
            self.epoch = parse_datetime(epoch_string)
            self.time_unit = time_unit_to_delta_map.get(time_unit_string)


def get_nc_attribute(
    variable: Variable, attribute_name: str,
    default_value: Optional[NetCDF4Attribute] = None
) -> Optional[NetCDF4Attribute]:
    """ A helper function that attempts to retrieve the value of a metadata
        attribute. If that attribute is missing the resulting `AttributeError`
        is handled and an optional default value is applied.

    """
    try:
        nc_attribute = variable.getncattr(attribute_name)
    except AttributeError:
        nc_attribute = default_value

    return nc_attribute


def resolve_reference_path(variable: Variable, reference: str) -> str:
    """ Extract the full path of a dimension reference based upon the path of
        the variable containing the reference.

        If the reference has a leading slash, it is assumed to already be
        qualified. If the group containing the variable making the reference
        has a variable matching that name, then assume the reference is to that
        matching variable in the group.

    """
    if reference.startswith('/'):
        output_reference = reference
    else:
        group = variable.group()
        if reference in group.variables and isinstance(group, Group):
            output_reference = '/'.join([group.path, reference])
        else:
            output_reference = f'/{reference}'

    return output_reference


def is_variable_in_dataset(variable_full_path: str, dataset: Dataset) -> bool:
    """ Check if a variable is present in the full dataset, based on its
        full path. Attempting to address a nested variable from a `Dataset`
        using its full path will result in an `IndexError` otherwise, even if
        trying to perform `if variable_name in dataset`.

        If a group in the path is missing, the check will also return `False`.

    """
    variable_parts = variable_full_path.lstrip('/').split('/')
    current_group = dataset

    while len(variable_parts) > 1:
        nested_group = variable_parts.pop(0)
        if nested_group in current_group.groups:
            current_group = current_group.groups[nested_group]
        else:
            variable_parts = []

    return (
        len(variable_parts) == 1
        and variable_parts[0] in current_group.variables
    )
