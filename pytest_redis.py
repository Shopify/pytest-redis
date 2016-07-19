"""pytest-redis queue plugin implementation."""
import os

import redis
import pytest
import itertools

from _pytest.terminal import TerminalReporter
import _pytest.runner
from _pytest.main import NoMatch
from _pytest.main import EXIT_NOTESTSCOLLECTED, EXIT_OK


def pytest_addoption(parser):
    """Add command line options to py.test command."""
    parser.addoption('--redis-host', metavar='redis_host',
                     type=str, help='The host of the redis instance.',
                     required=True)
    parser.addoption('--redis-port', metavar='redis_port',
                     type=str, help='The port of the redis instance.',
                     required=True)
    parser.addoption('--redis-list-key', metavar='redis_list_key',
                     type=str,
                     help=('The key of the redis list containing '
                           'the test paths to execute.'),
                     required=True)
    parser.addoption('--redis-backup-list-key',
                     metavar='redis_backup_list_key',
                     type=str,
                     default=None,
                     help=('The key of the redis list where tests '
                           'that have been ran are pushed to. If the main '
                           'redis list specified by redis-list-key is empty '
                           'then this list is polled for the next tests.'
                           'If the main redis-list-key is not empty then ran '
                           'tests are pushed to this list.'),
                     required=False)

def retrieve_test_from_redis(redis_connection, list_key, backup_list_key):
    """Remove and return a test path from the redis queue."""
    if backup_list_key is not None:
        return redis_connection.rpoplpush(list_key, backup_list_key)
    else:
        return redis_connection.rpop(list_key)



def pytest_collection(session, genitems=True):
    """We hook into the collection call and do the collection ourselves."""
    hook = session.config.hook
    try:
        items = perform_collect_and_run(session)
    finally:
        hook.pytest_collection_finish(session=session)
    session.testscollected = len(items)
    return items


def get_redis_connection(config):
    """Get a redis connection base on config args."""
    redis_host = config.getoption('redis_host')
    redis_port = config.getoption('redis_port')
    r_client = redis.StrictRedis(host=redis_host,
                                 port=redis_port)
    return r_client


def populate_test_generator(session, redis_connection):
    """Create a test path generator that consumes from the main redis list.

    This first checks the backup list for any entries and pushes them to the main
    redis list before returning a generator to that list.
    """
    redis_list_key = session.config.getoption("redis_list_key")
    backup_list_key = session.config.getoption("redis_backup_list_key")

    if backup_list_key is not None and redis_connection.llen(backup_list_key) != 0:
        # Push tests to the main redis list
        while redis_connection.rpoplpush(backup_list_key, redis_list_key) is not None:
            continue

    return redis_test_generator(session.config,
                                redis_connection,
                                redis_list_key,
                                backup_list_key=backup_list_key)




def perform_collect_and_run(session):
    """Collect and run tests streaming from the redis queue."""
    # This mimics the internal pytest collect loop, but shortened
    # while running tests as soon as they are found.
    term = TerminalReporter(session.config)

    redis_connection = get_redis_connection(session.config)

    redis_list = populate_test_generator(session,
                                         redis_connection)


    default_verbosity = session.config.option.verbose
    hook = session.config.hook
    session._initialpaths = set()
    session._initialparts = []
    session._notfound = []
    session.items = []
    for arg in redis_list:
        term.write(os.linesep)
        parts = session._parsearg(arg)
        session._initialparts.append(parts)
        session._initialpaths.add(parts[0])
        arg = "::".join(map(str, parts))
        session.trace("processing argument", arg)
        session.trace.root.indent += 1
        try:
            for x in session._collect(arg):
                items = session.genitems(x)
                new_items = []
                for item in items:
                    new_items.append(item)

                # HACK ATTACK: This little hack lets us remove the
                # 'collected' and 'collecting' messages while still
                # keeping the default verbosity for the rest of the
                # run...
                session.config.option.verbose = -1
                hook.pytest_collection_modifyitems(session=session,
                                                   config=session.config,
                                                   items=new_items)
                session.config.option.verbose = default_verbosity
                for item in new_items:
                    session.items.append(item)

                def getnextitem(i):
                    # this is a function to avoid python2
                    # keeping sys.exc_info set when calling into a test
                    # python2 keeps sys.exc_info till the frame is left
                    try:
                        print "Getting next"
                        return new_items[i+1]
                    except IndexError:
                        print "Returning none"
                        return None

                for i, item in enumerate(new_items):
                    nextitem = getnextitem(i)
                    _pytest.runner.pytest_runtest_protocol(item=item, nextitem=nextitem)
                    # _pytest.runner.pytest_runtest_protocol(item, None)

        except NoMatch:
            # we are inside a make_report hook so
            # we cannot directly pass through the exception
            raise pytest.UsageError("Could not find" + arg)
        session.trace.root.indent -= 1
    return session.items


def redis_test_generator(config, redis_connection, redis_list_key,
                         backup_list_key=None):
    """A generator that pops and returns test paths from the redis list key."""
    term = TerminalReporter(config)

    val = retrieve_test_from_redis(redis_connection,
                                   redis_list_key,
                                   backup_list_key)

    if val is None:
        term.write("No items in redis list '%s'\n" % redis_list_key)

    while val is not None:
        yield val
        val = retrieve_test_from_redis(redis_connection,
                                       redis_list_key,
                                       backup_list_key)


# def pytest_runtest_protocol(item, nextitem):
#     """Called when an item is run. Returning true stops the hook chain."""
#     return True


def pytest_sessionfinish(session, exitstatus):
    """Called when the entire test session is completed."""
    # adjust the return value to return EXIT_OK
    # when no tests are collected.
    if session.exitstatus == EXIT_NOTESTSCOLLECTED:
        session.exitstatus = EXIT_OK
        return EXIT_OK
    else:
        return session.exitstatus
