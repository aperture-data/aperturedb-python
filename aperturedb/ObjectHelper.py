def dict_to_obj(d, name):
    """
    Converts a dict into native object, with keys as properties.
    Expects a name to be used for the generated type.
    """
    top = type(name, (object,), d)
    seqs = tuple, list, set, frozenset
    for i, j in d.items():
        if isinstance(j, dict):
            setattr(top, i, dict_to_obj(j))
        elif isinstance(j, seqs):
            setattr(top, i, 
                type(j)(dict_to_obj(sj, name) if isinstance(sj, dict) else sj for sj in j))
        else:
            setattr(top, i, j)
    return top