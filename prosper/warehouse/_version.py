"""_version.py: version information for writing library"""
INSTALLED = True
try:    #pragma: no cover
    import semantic_version
except ImportError:
    INSTALLED = False

__version__ = '1.0.0'
# major.minor.patch-prerelease

def semantic_to_numeric(version_string):
    """translate sematic version to a numeric value

    Args:
        version_string (str): semantic version number

    Returns:
        (float) xxxxyyyyzzzz.pppp
        x=major
        y=minor
        z=patch
        p=prerelease

    """
    if not INSTALLED:
        return -1.0
    sem_v = semantic_version.Version(version_string)

    number_string = '{major:04d}{minor:04d}{patch:04d}'.format(
        major=sem_v.major,
        minor=sem_v.minor,
        patch=sem_v.patch
    )

    if sem_v.prerelease:
        number_string = '{existing}.{prerelease:04d}'.format(
            existing=number_string,
            prerelease=int(sem_v.prerelease[0]) + 1
        )
    else:
        number_string = '{existing}.0000'.format(
            existing=number_string
        )

    numeric_val = float(number_string)
    return numeric_val

__version_int__ = semantic_to_numeric(__version__)
