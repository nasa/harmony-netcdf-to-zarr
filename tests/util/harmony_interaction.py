"""
Utility methods for Harmony interactions such as creating valid messages and environments,
and parsing callbacks.
"""

import json
from urllib import parse

# Mock environment so .env does not need correct values
MOCK_ENV = dict(
    ENV='test',
    EDL_USERNAME='fake',
    EDL_PASSWORD='fake',
    SHARED_SECRET_KEY='XYZZYXYZZYXYZZYXYZZYXYZZYXYZZYXY',
    AWS_DEFAULT_REGION='us-west-2'
)


def mock_message() -> str:
    """ Returns a minimal Harmony message. This has been updated to remove the
        granules from `sources`, as these have been deprecated in favour of
        STAC catalog entries.

        Returns
        -------
        string
            A JSON string containing the Harmony message
    """
    return json.dumps({
        'user': 'jdoe',
        'callback': 'http://localhost/fake',
        'stagingLocation': 's3://example-bucket/public/harmony/netcdf-to-zarr/example-uuid/',
        'sources': [{'collection': 'C000-TEST'}],
        'format': {'mime': 'application/x-zarr'}
    })


def parse_callbacks(_callback_post):
    """
    Given a mock of Harmony's _callback_post method, which takes a callback URL as its only argument,
    returns a list of dictionaries of the GET parameters for each callback invocation.

    Example:
        >>> _callback_post('/response?a=x&b=y')
        >>> _callback_post('/response?c=z')
        >>> parse_callbacks(_callback_post)
        [{'a': 'x', 'b': 'y'}, {'c': 'z'}]

    Parameters
    ----------
    _callback_post : unittest.mock.Mock
        A mock of Harmony's _callback_post method, which has been invoked in test conditions

    Returns
    -------
    dict[]
        An array of the parsed query parameters from each call
    """
    result = []
    for args, kwargs in _callback_post.call_args_list:
        url = args[0]
        query = parse.urlsplit(url).query
        parsed_query = {k: v[0] for k, v in parse.parse_qs(query).items()}
        result.append(parsed_query)
    return result
