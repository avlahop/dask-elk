def make_sequence(item):
    if isinstance(item, (list, tuple)):
        return item
    return list(item)