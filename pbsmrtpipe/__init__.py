VERSION = (0, 46, 2)


def get_version():
    return ".".join([str(i) for i in VERSION])

__version__ = get_version()


def get_changelist():
    # Legacy from the perforce era, but keeping this. It's not worth breaking
    return "UnknownChangelist"
