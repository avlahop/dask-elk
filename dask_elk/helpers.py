def make_sequence(elements):
    if isinstance(elements, (list, set, tuple)):
        return elements
    elif elements is None:
        return []
    else:
        return [elements]
