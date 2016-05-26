"""Utilities for tests."""


def default_pytest_redis_args():
    """Return default options for each pytest execution."""
    return ['-v', '-p', 'pytest_redis']


def get_option_array(option_dict):
    """Return cmdline options for a dict in '--key=val' form."""
    return ["--{}={}".format(k, v) for k, v in option_dict.items()]


def create_test_file(testdir, filename, text):
    """Create test file with the given name and text contents."""
    the_kwargs = {
        filename: text
    }
    ext = ""
    try:
        ext = filename.split(".")[-1]
    except:
        pass

    testdir.makefile(ext, **the_kwargs)


def get_standard_args(option_dict):
    """Return standard pytest redis_args combinbed with option dict."""
    return default_pytest_redis_args() + get_option_array(option_dict)


def create_test_dir(testdir, dirname):
    """Create test file with the given name and text contents."""
    testdir.mkdir(dirname)
