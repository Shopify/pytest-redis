"""Tests the pytest-redis with a running redis instance.

Tests should be launched from the root directory with:
py.test --redis-port=<port> --redis-host=<host> --redis-list-key=<list_to_use>

"""
import multiprocessing
from multiprocessing import Pipe
import os.path

from _pytest.main import (EXIT_OK,
                          EXIT_TESTSFAILED,
                          EXIT_INTERRUPTED,
                          EXIT_USAGEERROR)

import utils


def setup_multiple_consumer_processes(testdir, py_test_args, num_threads):
    """Return multiple threads that will run pytest with the given args."""
    def run_consumer(testdir, test_args, pipe_to_parent):
        result = testdir.runpytest(*test_args)
        pipe_to_parent.send([result.ret, result.outlines])

    ret = []
    for i in range(num_threads):
        parent_pipe_end, child_pipe_conn = Pipe()
        process = multiprocessing.Process(target=run_consumer,
                                          args=[testdir,
                                                py_test_args,
                                                child_pipe_conn])
        ret.append((process, parent_pipe_end))
    return ret


def get_elaborate_test_directories_and_paths(testdir, folders):
    """Create an elaborate depth=1 directory structure."""
    num_test_methods_per_file = 2
    # Create the contents of the test files
    for folder in folders.keys():
        utils.create_test_dir(testdir, folder)
        for a_file in folders[folder]:
            file_path = folder + "/" + a_file
            file_text = ""
            for test_num in range(num_test_methods_per_file):
                test_name = "test_multiple_consumers_{}_{}".format(
                    folder + "_" + a_file.split(".")[0],
                    test_num)
                file_text += """
                def {}():
                    assert True
                """.format(test_name)
                # We push each test to the queue
                # `num_times_to_run_tests` times to
                # have a bit more work to do
                test_name_path = folder + "/" + a_file + "::" + test_name
                yield test_name_path
            utils.create_test_file(testdir, file_path, file_text)


def test_external_arguments(testdir, redis_connection, redis_args):
    """Ensure that the plugin doesn't intefere with other plugins."""
    test_file_name = "test_external_arguments.py"
    utils.create_test_file(testdir, test_file_name, """
        def test_run_should_run():
            assert True
    """)
    redis_connection.lpush(redis_args['redis-list-key'],
                           test_file_name + "::test_run_should_run")

    junitxml_filename = "pytest.xml"
    py_test_args = utils.get_standard_args(redis_args) + \
        ['--junitxml=' + junitxml_filename]

    junitxml_path = str(testdir.tmpdir) + "/" + junitxml_filename

    result = testdir.runpytest(*py_test_args)
    assert os.path.exists(junitxml_path)
    assert result.ret == EXIT_OK


def test_elborate_test_modules(testdir, redis_connection, redis_args):
    """Ensure that modules in are collected and executed correctly."""
    num_times_to_run_each_tests = 2
    test_to_run_list = []
    folders = {'module1': ["file1.py"],
               'module2': ["file2.py"],
               'module3': ["file3.py"]}

    test_path_generator = get_elaborate_test_directories_and_paths(testdir,
                                                                   folders)
    for test_path in test_path_generator:
        for _ in range(num_times_to_run_each_tests):
            redis_connection.lpush(redis_args['redis-list-key'],
                                   test_path)
            test_to_run_list.append(test_path)

    py_test_args = utils.get_standard_args(redis_args)
    result = testdir.runpytest(*py_test_args)
    result.stdout.fnmatch_lines(["*" + test_string + " PASSED"
                                 for test_string in test_to_run_list]
                                )


def test_multiple_consumers(testdir, redis_connection, redis_args):
    """Pull tests from multiple test runs simultaneously."""
    test_file_name = "test_multiple_consumers.py"

    utils.create_test_file(testdir, test_file_name, """
        def test_multiple_consumers():
            assert True
    """)
    for i in range(100):
        redis_connection.lpush(redis_args['redis-list-key'],
                               test_file_name + "::test_multiple_consumers")

    py_test_args = utils.get_standard_args(redis_args)

    processes = setup_multiple_consumer_processes(testdir, py_test_args, 5)

    for proc in processes:
        proc[0].start()

    for proc in processes:
        proc[0].join()
        exit_result = proc[1].recv()[0]

        assert exit_result == EXIT_OK

    assert redis_connection.llen(redis_args['redis-list-key']) == 0


def test_no_consumption_of_item(testdir, redis_args):
    """Make sure that we don't run tests when the list is empty."""
    test_file_name = "test_not_used"
    utils.create_test_file(testdir, test_file_name, """
        def test_run_should_run():
            assert True
    """)
    py_test_args = utils.get_standard_args(redis_args)
    result = testdir.runpytest(*py_test_args)
    assert result.ret == EXIT_OK


def test_non_existent_test_name(testdir, redis_connection, redis_args):
    """Entire test should fail if a non-existent test is specfied."""
    test_file_name = "test_name.py"
    utils.create_test_file(testdir, test_file_name, """
        def test_name_dne():
            assert True
    """)

    redis_connection.lpush(redis_args['redis-list-key'],
                           test_file_name + "::test_wrong_name")

    py_test_args = utils.get_standard_args(redis_args)
    result = testdir.runpytest(*py_test_args)
    assert result.ret == EXIT_USAGEERROR


def test_module_test_name(testdir, redis_connection, redis_args):
    """Test path as a module name."""
    module_1_name = "test_module_1"
    module_1_test_filename = "test_module_1_file.py"
    module_1_test_filename_contents = """
        def test_exists():
            assert True
        def test_does_exist():
            assert True
        def test_random_test():
            assert True
    """
    module_2_name = "test_module_2"
    module_2_test_filename = "test_module_2_file.py"
    module_2_test_filename_contents = """
        def test_exists():
            assert True
        def test_does_exist_2():
            assert True
        def test_random_test_2():
            assert True
    """
    utils.create_test_dir(testdir, module_1_name)
    utils.create_test_file(testdir, module_1_name + "/" +
                           module_1_test_filename,
                           module_1_test_filename_contents)
    utils.create_test_dir(testdir, module_2_name)
    utils.create_test_file(testdir, module_2_name + "/" +
                           module_2_test_filename,
                           module_2_test_filename_contents)
    py_test_args = utils.get_standard_args(redis_args)

    for module in [module_1_name, module_2_name]:
        redis_connection.lpush(redis_args['redis-list-key'], module)
        result = testdir.runpytest(*py_test_args)
        result.stdout.fnmatch_lines([
            "*" + module + "*PASSED",
            "*" + module + "*PASSED"
        ])
        assert result.ret == EXIT_OK


def test_conf_tests(testdir, redis_connection, redis_args):
    """Ensure that conftest.py are executed."""
    test_file_name = "test_conf_test.py"

    fixture_prints = [
        "fixture_1_ran",
        "fixture_2_ran"
    ]
    num_times_to_run_tests = 3

    utils.create_test_file(testdir, test_file_name, """
        def test_run0(a_fixture):
            assert not a_fixture

        def test_run1(another_fixture):
            assert another_fixture

        def test_run2():
            assert False
    """)

    test_file_name_2 = "conftest.py"
    utils.create_test_file(testdir, test_file_name_2, """
        import pytest

        def pytest_collection_modifyitems(session, config, items):
            if "test_run2" in items[0].nodeid:
                items[:] = []

        @pytest.fixture
        def a_fixture():
            print
            print "{}"
            return False

        @pytest.fixture
        def another_fixture():
            print
            print "{}"
            return True

    """.format(*fixture_prints))

    for i in range(num_times_to_run_tests):
        redis_connection.lpush(redis_args['redis-list-key'],
                               test_file_name)

    py_test_args = utils.get_standard_args(redis_args) + ["-s"]

    result = testdir.runpytest(*py_test_args)

    assert result.ret == EXIT_TESTSFAILED
    result.stdout.fnmatch_lines([test for test in fixture_prints] *
                                num_times_to_run_tests)


def test_run_once(testdir, redis_connection, redis_args):
    """Ensure that collected tests are only executed a single time."""
    module_name = "test_run_once_module"
    module_test_filename = "test_run_once.py"
    module_test_filename_contents = """
        import local
        def test_exists():
            assert True
        def test_does_exist():
            assert True
        def test_random_test():
            assert True
    """
    utils.create_test_dir(testdir, module_name)
    utils.create_test_file(testdir, module_name + "/" + module_test_filename,
                           module_test_filename_contents)
    utils.create_test_file(testdir, module_name + "/__init__.py",
                           "")
    utils.create_test_file(testdir, module_name + "/local.py",
                           """
                           print "Imported"
                           """)

    py_test_args = utils.get_standard_args(redis_args) + ["-s"]
    redis_connection.lpush(redis_args['redis-list-key'],
                           module_name + "/" +
                           module_test_filename + "::test_exists")
    redis_connection.lpush(redis_args['redis-list-key'],
                           module_name + "/" +
                           module_test_filename + "::test_does_exist")
    redis_connection.lpush(redis_args['redis-list-key'],
                           module_name + "/" +
                           module_test_filename + "::test_random_test")
    # We've pushed 3 tests from the same file,
    # we should import local.py only once
    result = testdir.runpytest(*py_test_args)
    result.stdout.fnmatch_lines([
        "*Imported*",
        "*test_exists PASSED",
        "*test_does_exist PASSED",
        "*test_random_test PASSED"
    ])
    assert result.ret == EXIT_OK
