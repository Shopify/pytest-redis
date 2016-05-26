"""Tests the pytest-redis num collect args."""

import utils


def create_test_file(testdir, num_tests):
    """Create test file and return array of paths to tests."""
    test_filename = "test_file.py"
    test_filename_contents = ""
    for i in range(num_tests):
        test_filename_contents += """
        def test_func_{}():
            assert True
        """.format(i)

    utils.create_test_file(testdir, test_filename, test_filename_contents)
    return [test_filename + "::test_func_{}".format(i) for i in range(num_tests)]


def get_args_for_num_collect(redis_args, num):
    """Return args for the num collect tests."""
    args = utils.get_standard_args(redis_args) + ["-s",
                                                  "--redis_num_to_collect=" +
                                                  str(num)]
    return [arg for arg in args if '--redis-backup-list-key=' not in arg]


def test_num_collect_tests(testdir, redis_connection,
                           redis_args):
    """Ensure that the backup list is filled with tests."""
    file_paths_to_test = create_test_file(testdir, 20)
    py_test_args = get_args_for_num_collect(redis_args, 1)

    [redis_connection.lpush(redis_args['redis-list-key'], file)
        for file in file_paths_to_test]

    assert redis_connection.llen(redis_args['redis-list-key']) == 20

    result = testdir.runpytest(*py_test_args)
    result.stdout.fnmatch_lines(file + " PASSED"for file in file_paths_to_test)
    assert redis_connection.llen(redis_args['redis-list-key']) == 0

    py_test_args = get_args_for_num_collect(redis_args, 2)

    [redis_connection.lpush(redis_args['redis-list-key'], file)
        for file in file_paths_to_test]

    assert redis_connection.llen(redis_args['redis-list-key']) == 20
    testdir.runpytest(*py_test_args)
    result.stdout.fnmatch_lines(file + " PASSED"for file in file_paths_to_test)
    assert redis_connection.llen(redis_args['redis-list-key']) == 0
