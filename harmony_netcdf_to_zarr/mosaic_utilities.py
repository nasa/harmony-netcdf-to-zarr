""" A module including functions to take many input NetCDF-4 datasets and
    create aggregated dimensions, allowing for a single Zarr output.

"""
from datetime import timedelta
from typing import Dict, List, Optional, Tuple, Union

from cftime import date2num, num2date
from dateutil.parser import parse as parse_datetime
from netCDF4 import Dataset, Group, Variable
import numpy as np

from harmony_netcdf_to_zarr.exceptions import MixedDimensionTypeError


BoundsTuple = Tuple[Optional[str], Optional[np.ndarray]]
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
    def __init__(self, dimension_path: str, dimension_values: np.ndarray,
                 dimension_units: str, bounds_path: str = None,
                 bounds_values: np.ndarray = None):
        self.dimension_path = dimension_path
        self.values = dimension_values
        self.units = dimension_units
        self.bounds_path = bounds_path
        self.bounds_values = bounds_values
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

    def is_temporal(self):
        """ Return whether the instance could extract all required information from
            the `units` metadata attribute to define dimension as temporal.

        """
        return self.time_unit is not None and self.epoch is not None

    def get_values(self, output_units: Optional[str] = None) -> np.ndarray:
        """ Retrieve dimension values. If the dimension is temporal, and an
            output epoch is specified via units (e.g., 'seconds since 2020-01-01'),
            then convert the temporal values to use the epoch from that string.
            If either the dimension is non-temporal, or a new epoch is not
            specified, return the input dimension values without changing them.

        """
        if self.is_temporal() and output_units is not None:
            values = date2num(num2date(self.values, self.units), output_units)
        else:
            values = self.values

        return values


class NetCDF4DimensionInformation(DimensionInformation):
    """ A subclass of the `DimensionInformation` class that takes a NetCDF-4
        file and extracts the required information from the NetCDF-4 variable
        in order to create the class instance.

    """
    def __init__(self, dataset: Dataset, dimension_path: str):
        dimension_variable = dataset[dimension_path]
        bounds_path, bounds_values = self._get_bounds(dataset,
                                                      dimension_variable)
        super().__init__(dimension_path, dimension_variable[:],
                         get_nc_attribute(dimension_variable, 'units'),
                         bounds_path, bounds_values)

    def _get_bounds(self, dataset: Dataset, dimension: Variable) -> BoundsTuple:
        """ A helper method to check for the `bounds` attribute in the
            dimension metadata. Following the CF-Conventions for a 1-D
            dimension variable, it is expected that the `bounds` attribute will
            be a reference to another variable in the NetCDF-4 file.
            If present return the 2-D array.

        """
        bounds_attribute = get_nc_attribute(dimension, 'bounds')

        if bounds_attribute is not None:
            bounds_path = resolve_reference_path(dimension, bounds_attribute)
            bounds_values = dataset[bounds_path][:]
        else:
            bounds_path = None
            bounds_values = None

        return bounds_path, bounds_values


class DimensionsMapping:
    """ A class containing the information for all dimensions contained in each
        of the input NetCDF-4 granules. This class also will produce single,
        aggregated arrays and metadata (if required) for the output Zarr
        object. Note - output aggregated arrays are only calculated if there is
        more than one input granule. This ensures that the NetCDF-to-Zarr
        service is compatible with single-granule requests, where those
        granules contain irregular dimensions that would otherwise be converted
        to a dimension with consistent spacing between all pixels.

    """
    def __init__(self, input_paths: List[str]):
        self.input_paths = input_paths
        self.input_dimensions = {}
        self.output_dimensions = {}
        self.output_bounds = {}
        self._map_input_dimensions()

        if len(self.input_paths) > 1:
            # Only calculate regular, aggregated dimensions for multiple inputs
            self._aggregate_output_dimensions()

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
                    dim_data[input_path] = NetCDF4DimensionInformation(
                        dataset, dimension_path
                    )

    def _aggregate_output_dimensions(self):
        """ Iterate through each input dimension listed in
            `self.input_dimensions`:

            * Iterate through the input_dimensions attributes. The top level of
              that dictionary corresponds to a dimension (keys are full paths).
            * For each dimension, check the dimension in all the input granule
              is the same type (temporal or non-temporal).
            * For temporal dimensions, make sure to get all the input values
              relative to the same epoch.
            * Find the resolution of the output grid using the greatest common
              divisor.
            * Calculate the output grid from the resolution.
            * Save the aggregated dimension to the output_dimension class
              attribute.

        """
        for dimension_name, dimension_inputs in self.input_dimensions.items():
            are_inputs_temporal = [dimension_input.is_temporal()
                                   for dimension_input
                                   in dimension_inputs.values()]

            if all(are_inputs_temporal):
                # All input granule dimensions with this path are temporal.
                # Temporal dimensions have units e.g. `seconds since <epoch>`,
                # requiring unifying to a common epoch.
                self.output_dimensions[dimension_name] = self._get_temporal_output_dimension(
                    dimension_inputs, dimension_name
                )
                self._map_output_bounds(self.output_dimensions[dimension_name])
            elif any(are_inputs_temporal):
                raise MixedDimensionTypeError(dimension_name)
            else:
                # Temporarily comment out non-temporal aggregation as there are
                # issues with Swath Projector output not always being on a
                # perfectly spaced grid
                # All input granule dimensions with this path are non-temporal
                # That means that the raw values are likely all the same units.
                # all_input_values = np.unique(
                #     np.concatenate([dimension_input.get_values()
                #                     for dimension_input
                #                     in dimension_inputs.values()])
                # )

                # Because it is assumed the units are the same for all inputs,
                # use the `units` metadata from the first input granule as
                # the value for the aggregated output dimension.
                # output_dimension_units = next(iter(dimension_inputs.values())).units

                # self.output_dimensions[dimension_name] = self._get_output_dimension(
                #     dimension_name, all_input_values, output_dimension_units
                # )
                # self._map_output_bounds(self.output_dimensions[dimension_name])
                pass

    def _get_temporal_output_dimension(self,
                                       dimension_inputs: Dict[str, DimensionInformation],
                                       dimension_name: str) -> DimensionInformation:
        """ Find the units metadata attribute for the input granule with the
            earliest epoch. Apply this epoch to the temporal data in all
            granules, to place them with respect to a common epoch. Then use
            generate an output dimension grid.

        """
        dimension_units = [dimension_input.units
                           for dimension_input in dimension_inputs.values()]
        dimension_epochs = [dimension_input.epoch
                            for dimension_input in dimension_inputs.values()]

        output_dimension_units = dimension_units[np.argmin(dimension_epochs)]

        all_input_values = np.unique(
            np.concatenate(
                [dimension_input.get_values(output_dimension_units)
                 for dimension_input in dimension_inputs.values()],
                dtype=list(dimension_inputs.values())[0].get_values().dtype
            )
        )

        return self._get_output_dimension(dimension_name, all_input_values,
                                          output_dimension_units)

    def _get_output_dimension(self, dimension_name: str,
                              input_dimension_values: np.ndarray,
                              dimension_units: str) -> DimensionInformation:
        """ Use `get_resolution` to determine the greatest common divisor of
            all dimension input values. Calculate the output dimension values
            using this resolution, the minimum value of the input dimensions
            and maximum value of the input dimensions.

            Next  use the `_get_dimension_bounds` class method to check if the
            input dimensions have an associated bounds variable, if so derive
            the bounds values for the output dimension variable.

            Finally return a new `DimensionInformation` object encapsulating an
            aggregated output dimension, which uses a regularly spaced grid
            that extends to include all input dimension values.

        """
        grid_resolution = get_resolution(input_dimension_values)
        output_dimension_values = get_grid_values(input_dimension_values,
                                                  grid_resolution)

        bounds_path, bounds_values = self._get_dimension_bounds(
            dimension_name, output_dimension_values
        )

        return DimensionInformation(dimension_name, output_dimension_values,
                                    dimension_units, bounds_path,
                                    bounds_values)

    def _get_dimension_bounds(self, dimension_name,
                              output_dimension_values) -> BoundsTuple:
        """ Check if the input dimension in the first input granule had
            associated bounds data. If so, assign the input bounds values to
            the corresponding grid points in the output dimension values.

            Next check for values in the output dimension that do not have
            bounds data (due to gaps in coverage, e.g., temporally
            non-consecutive input granules). If there are missing bounds values
            for the output dimension, use the median difference between the
            input bounds and dimension values to derive the missing values.

            If the input dimension data did not have an associated bounds
            variable, this method with return `None` for both the values and
            path of the bounds.

        """
        bounds_path = next(
            iter(self.input_dimensions[dimension_name].values())
        ).bounds_path

        if bounds_path is not None:
            bounds_values = np.zeros((output_dimension_values.size, 2),
                                     dtype=output_dimension_values.dtype)
            filled_indices = set()

            for input_dimension in self.input_dimensions[dimension_name].values():
                # For each granule, match the dimension values to the output
                # dimension values. Place the bounds values from the input
                # granules in the corresponding places in the output bounds
                # 2-D array. Also accumulate indices where bounds are applied,
                # so that any missing bounds data can be identified.
                input_indices = np.where(np.in1d(output_dimension_values,
                                                 input_dimension.values))[0]

                bounds_values[input_indices, :] = input_dimension.bounds_values[:]
                filled_indices.update(input_indices)

            if len(filled_indices) != output_dimension_values.size:
                # The output bounds array has not been entirely filled
                all_indices = set(range(output_dimension_values.size))
                unfilled_indices = np.array(list(all_indices - filled_indices))
                filled_indices = np.array(list(filled_indices))

                # Find median differences between the input dimension values
                # and the upper and lower bounds where the bounds are filled
                median_lower = np.median(
                    np.subtract(output_dimension_values,
                                bounds_values.T[0])[filled_indices]
                )
                median_upper = np.median(
                    np.subtract(bounds_values.T[1],
                                output_dimension_values)[filled_indices]
                )

                # Apply the median differences to each point with missing
                # bounds data to fill the rest of the bounds array.
                bounds_values[unfilled_indices, 0] = np.subtract(
                    output_dimension_values[unfilled_indices], median_lower
                )
                bounds_values[unfilled_indices, 1] = np.add(
                    output_dimension_values[unfilled_indices], median_upper
                )

            # Remove any recurring decimal places:
            _, scale_factor = scale_to_integers(output_dimension_values)
            bounds_decimals = int(np.log10(scale_factor)) + 1
            bounds_values = np.around(bounds_values, decimals=bounds_decimals)
        else:
            bounds_values = None

        return bounds_path, bounds_values

    def _map_output_bounds(self, dimension: DimensionInformation):
        """ Create a mapping from the fully resolved bounds variable path to
            the fully resolved dimension variable path. This allows for easier
            information retrieval when only the bounds path is known.

        """
        if dimension.bounds_path is not None:
            self.output_bounds[dimension.bounds_path] = dimension.dimension_path


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


def scale_to_integers(input_floats: np.ndarray) -> Tuple[np.ndarray, float]:
    """ A function that ensures all values in the input array are scaled
        by a power of ten that ensures they are all integers. Integers are
        required to use the `numpy.gcd` function.

        To avoid an infinite while loop, the smallest value in the input
        array, which will be the differences between dimension values, is
        assumed to be 10^10, which should include temporal differences
        beyond the nanosecond level.

        `np.round` ensures that recurring decimals, such as 0.999999
        will return 1, rather than 0.

    """
    scaled_values = input_floats[:].copy()
    scale_factor = 1.0
    max_scale_factor = 1e10

    while (
        any(np.not_equal(scaled_values, scaled_values.astype(int)))
        and scale_factor < max_scale_factor
    ):
        scaled_values = scaled_values * 10
        scale_factor *= 10.0

    return np.round(scaled_values).astype(int), scale_factor


def get_resolution(dimension_values: np.ndarray):
    """ Use the `numpy.gcd` function to calculate the resolution of an
        input grid.

        First this function scales all the input values to integers such that
        all significant figures are preserved (e.g., 10.325 becomes 10325).
        Next the difference between the scaled smallest dimension value and all
        others are calculated. The `numpy.gcd` function, which requires
        integer input, is used to find the greatest common divisor of these
        scaled integers, which represents the output grid resolution scaled by
        the same factor used to create the integer difference. Finally, the
        greatest common divisor is divided by the scale factor to retrieve the
        grid resolution in the domain of the original dimension values.

    """
    scaled_values, scale_factor = scale_to_integers(dimension_values)
    scaled_diff_pairs = np.subtract(
        scaled_values,
        np.multiply(np.ones_like(dimension_values), scaled_values.min())
    )
    non_zero_diffs = scaled_diff_pairs[scaled_diff_pairs.nonzero()].astype(int)
    greatest_common_divisor = np.gcd.reduce(non_zero_diffs)
    return np.divide(greatest_common_divisor, scale_factor)


def get_grid_values(input_values: np.ndarray,
                    grid_resolution: np.floating) -> np.ndarray:
    """ Return a linearly spaced grid that extends from the minimum value of
        the input array to the maximum. The grid spacing will be the supplied
        resolution.

        The scale factor is used to ensure the `np.linspace` output does not
        include recurring decimals. For example, the GPM/IMERG longitude grid
        output dimension previously included -179.8499999999, instead of
        -179.85.

    """
    _, scale_factor = scale_to_integers(input_values)

    grid_max = input_values.max()
    grid_min = input_values.min()

    if grid_min != grid_max:
        n_grid_points = int(
            np.round(((grid_max - grid_min) / grid_resolution))
        ) + 1
    else:
        n_grid_points = 1

    grid_values = np.linspace(grid_min, grid_max, n_grid_points,
                              dtype=input_values.dtype)

    return np.around(grid_values, decimals=int(np.log10(scale_factor)))
