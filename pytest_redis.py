"""pytest-redis queue plugin implementation."""
import os

import redis
import pytest
import traceback

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
    parser.addoption('--redis-pop-type', metavar='redis_pop_type',
                     type=str,
                     help=('Indicates which side the of the redis list '
                           'a test is removed from.'),
                     choices=['RPOP', 'rpop', 'LPOP', 'lpop'],
                     default="RPOP")
    parser.addoption('--redis-list-key', metavar='redis_list_key',
                     type=str,
                     help=('The key of the redis list containing '
                           'the test paths to execute.'),
                     required=True)
    parser.addoption('--redis-max-num-tests', metavar='redis_max_num_tests',
                     type=int,
                     help=('The maximum number of tests to run.'),
                     default=-1)


def retrieve_test_from_redis(redis_connection, list_key, command):
    """Remove and return a test path from the redis queue."""
    val = None
    if command.lower() == "lpop":
        val = redis_connection.lpop(list_key)
    elif command.lower() == "rpop":
        val = redis_connection.rpop(list_key)
    return val


def pytest_collection(session, genitems=True):
    """We hook into the collection call and do the collection ourselves."""
    hook = session.config.hook
    try:
        items = perform_collect_and_run(session)
    finally:
        hook.pytest_collection_finish(session=session)
    session.testscollected = len(items)
    return items


def perform_collect_and_run(session):
    """Collect and run tests streaming from the redis queue."""
    # This mimics the internal pytest collect loop, but shortened
    # while running tests as soon as they are found.
    term = TerminalReporter(session.config)
    redis_list = redis_test_generator(session.config, session.config.args)
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
                hook.pytest_collection_modifyitems(session=session,
                                                   config=session.config,
                                                   items=new_items)
                for item in new_items:
                    session.items.append(item)
                    _pytest.runner.pytest_runtest_protocol(item, None)
        except NoMatch:
            # we are inside a make_report hook so
            # we cannot directly pass through the exception
            raise pytest.UsageError("Could not find" + arg)

        session.trace.root.indent -= 1
    return session.items


def redis_test_generator(config, args_to_prepend):
    """A generator that pops and returns test paths from the redis list."""
    term = TerminalReporter(config)

    redis_host = config.getoption('redis_host')
    redis_port = config.getoption('redis_port')
    redis_pop_type = config.getoption('redis_pop_type')
    redis_list_key = config.getoption('redis_list_key')
    redis_max_num_tests = config.getoption('redis_max_num_tests')

    r_client = redis.StrictRedis(host=redis_host,
                                 port=redis_port)

    val = retrieve_test_from_redis(r_client,
                                   redis_list_key,
                                   redis_pop_type)

    if val is None:
        term.write("No items in redis list '%s'\n" % redis_list_key)

    counter = 1

    while (val is not None) and (counter != redis_max_num_tests):
        yield val
        val = retrieve_test_from_redis(r_client,
                                       redis_list_key,
                                       redis_pop_type)
        counter += 1


def pytest_exception_interact(node, call, report):
    try:
        from collections import namedtuple
        import psutil
        _ntuple_diskusage = namedtuple('usage', 'total used free')

        def disk_usage(path):
            """Return disk usage statistics about the given path.

            Returned valus is a named tuple with attributes 'total', 'used' and
            'free', which are the amount of total, used and free space, in bytes.
            """
            st = os.statvfs(path)
            free = st.f_bavail * st.f_frsize
            total = st.f_blocks * st.f_frsize
            used = (st.f_blocks - st.f_bfree) * st.f_frsize
            return _ntuple_diskusage(total, used, free)

        print disk_usage('/')
        print psutil.phymem_usage()
        print "pytest_exception_interact 2323"
        print node
        print call.excinfo.type
        print call.excinfo.value
        exec_str = str(call.excinfo.value)
        print exec_str
        filename = exec_str[exec_str.index("/tmp"):]
        # st = os.stat(filename)
        # print st
        paths = filename.split("/")[1:]
        paths[0] = "/" + paths[0]
        print paths
        print os.listdir(paths[0])
        print os.stat(paths[0])
        print ""
        print os.listdir(paths[0] + "/" + paths[1])
        print os.stat(paths[0] + "/" + paths[1])
        print ""
        print call.excinfo.tb
        print traceback.print_tb(call.excinfo.tb)
        print call.excinfo.typename
        print call.excinfo.traceback
        print report
        return
    except:
        return

def pytest_internalerror(excrepr, excinfo):
    print "internal 2323"
    print excrepr
    print excinfo

def pytest_runtest_protocol(item, nextitem):
    """Called when an item is run. Returning true stops the hook chain."""
    return True


def pytest_sessionfinish(session, exitstatus):
    """Called when the entire test session is completed."""
    # adjust the return value to return EXIT_OK
    # when no tests are collected.
    if session.exitstatus == EXIT_NOTESTSCOLLECTED:
        session.exitstatus = EXIT_OK
        return EXIT_OK
    else:
        return session.exitstatus
