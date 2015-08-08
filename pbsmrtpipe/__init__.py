VERSION = (0, 14, 10)


def get_version():
    return ".".join([str(i) for i in VERSION])

__version__ = get_version()

# I don't really like this, but keeping it.
# Perforce RCS
# $Date: 2015/06/12 $
# $Revision: #5 $

_changelist = "$Change: 152329 $"


def _get_changelist(perforce_str):
    import re
    rx = re.compile(r'Change: (\d+)')
    match = rx.search(perforce_str)
    if match is None:
        v = 'UnknownChangelist'
    else:
        try:
            v = int(match.group(1))
        except (TypeError, IndexError):
            v = "UnknownChangelist"
    return v


def get_changelist():
    return _get_changelist(_changelist)
