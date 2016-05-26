"""Configuration of pytest tests."""
import pytest
import redis


pytest_plugins = 'pytester'


def pytest_addoption(parser):
    """Create pytest-redis test options."""
    import pytest_redis
    pytest_redis.pytest_addoption(parser)
    parser.addoption("--force", dest='feature', action='store_true',
                     help=('A boolean value indicating if existing'
                           'redis lists should be deleted before starting'
                           'tests'),
                     default=False)


def handle_existing_list(redis_connection, do_force_deletion, list_key):
    """Clear a list in a redis instance or warns the user about deletion."""
    if redis_connection.llen(list_key):
        if not do_force_deletion:
            exit_string = ("redis list '{}' exists in redis instance. "
                           "pass --force to ignore this and delete "
                           "list.").format(list_key)
            pytest.exit(exit_string)
        else:
            print "Deleting pre-exiting redis list '{}'".format(
                list_key)
            clean_list(redis_connection, list_key)


def clean_list(redis_connection, list_key):
    """Delete a list in the given redis instance."""
    return redis_connection.delete(list_key)


@pytest.fixture
def redis_host(request):
    """Return redis_host cmdline arg."""
    return request.config.getoption("--redis-host")


@pytest.fixture
def redis_port(request):
    """Return redis_port cmdline arg."""
    return request.config.getoption("--redis-port")


@pytest.fixture
def redis_list_key(request):
    """Return redis_list_key cmdline arg."""
    return request.config.getoption("--redis-list-key")


@pytest.fixture
def redis_backup_list_key(request):
    """Return redis_list_key cmdline arg."""
    return request.config.getoption("--redis-backup-list-key")


@pytest.fixture
def force_del_lists(request):
    """Return force_del_lists cmdline arg."""
    return request.config.getoption("--force")


@pytest.fixture
def redis_args(redis_host, redis_port, redis_list_key,
               redis_backup_list_key):
    """Return cmdline args for redis."""
    return {
        'redis-host': redis_host,
        'redis-port': redis_port,
        'redis-list-key': redis_list_key,
        'redis-backup-list-key': redis_backup_list_key
    }


@pytest.yield_fixture
def redis_connection(redis_args, force_del_lists):
    """Return redis_connection and check for empty redist-list-key."""
    r_client = redis.StrictRedis(host=redis_args['redis-host'],
                                 port=redis_args['redis-port'])
    handle_existing_list(r_client, force_del_lists,
                         redis_args["redis-list-key"])
    handle_existing_list(r_client, force_del_lists,
                         redis_args["redis-backup-list-key"])

    yield r_client
    clean_list(r_client, redis_args["redis-backup-list-key"])
    clean_list(r_client, redis_args["redis-list-key"])
