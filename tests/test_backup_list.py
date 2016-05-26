"""Tests the pytest-redis backup list arguments."""

import utils


def create_test_file(testdir):
    """Create test file and return array of paths to tests."""
    test_filename = "test_file.py"
    test_filename_contents = """
        def test_exists():
            assert True
        def test_does_exist():
            assert True
    """
    utils.create_test_file(testdir, test_filename, test_filename_contents)
    return [test_filename + "::test_exists", test_filename +
            "::test_does_exist"]


def get_args_for_backup_list(redis_args, backup_list_key):
    """Return args for the backup list tests."""
    return utils.get_standard_args(redis_args) + ["-s",
                                                  "--redis-backup-list-key=" +
                                                  backup_list_key]


def test_run_back_up_test(testdir, redis_connection,
                          redis_args):
    """Ensure that the backup list is filled with tests."""
    file_paths_to_test = create_test_file(testdir)
    back_up_list = redis_args["redis-backup-list-key"]
    py_test_args = get_args_for_backup_list(redis_args, back_up_list)

    for a_file in file_paths_to_test:
        redis_connection.lpush(back_up_list,
                               a_file)

    testdir.runpytest(*py_test_args)

    assert redis_connection.llen(back_up_list) == 2
    for a_file in file_paths_to_test:
        assert redis_connection.rpop(back_up_list) == a_file


def test_run_tests_multiple_times_with_backup(testdir, redis_connection,
                                              redis_args):
    """Run a test multiple times to ensure backup list is used."""
    file_paths_to_test = create_test_file(testdir)
    back_up_list = redis_args["redis-backup-list-key"]
    py_test_args = get_args_for_backup_list(redis_args, back_up_list)

    for a_file in file_paths_to_test:
        redis_connection.lpush(redis_args['redis-list-key'],
                               a_file)

    for i in range(10):
        result = testdir.runpytest(*py_test_args)
        result.stdout.fnmatch_lines([i + " PASSED" for i in file_paths_to_test])
        assert redis_connection.llen(back_up_list) == 2

    for a_file in file_paths_to_test:
        assert redis_connection.rpop(back_up_list) == a_file
