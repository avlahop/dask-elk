from typing import Any

import numpy


def make_sequence(elements):
    if isinstance(elements, (list, set, tuple)):
        return elements
    elif elements is None:
        return []
    else:
        return [elements]


def sanitize_data(data: Any) -> Any:  # pylint: disable=invalid-name,too-many-return-statements
    """
    Sanitize turns Numpy types into basic Python types so they
    can be serialized into JSON.
    """
    if isinstance(data, (str, float, int, bool)):
        # x is already serializable
        return data
    elif isinstance(data, numpy.ndarray):
        # array needs to be converted to a list
        return data.tolist()
    elif isinstance(data, numpy.number):  # pylint: disable=no-member
        # NumPy numbers need to be converted to Python numbers
        return data.item()
    elif isinstance(data, dict):
        # Dicts need their values sanitized
        return {key: sanitize_data(value) for key, value in data.items()}
    elif isinstance(data, (list, tuple)):
        # Lists and Tuples need their values sanitized
        return [sanitize_data(x_i) for x_i in data]
    elif hasattr(data, 'to_json'):
        return data.to_json()
    return data