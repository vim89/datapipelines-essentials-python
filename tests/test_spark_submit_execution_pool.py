import unittest
from datetime import datetime

from utils.Utilities import init_logging, create_multiprocess_pool, execute_bash


class TestSparkSubmitExecutionPool(unittest.TestCase):
    init_logging(datetime.now())

    def test_create_multiprocess_pool(self):
        bash_commands = [
            'echo "cmd1"',
            'echo "cmd2"',
            'echo "cmd3"',
            'hadoop version',
            'echo "cmd5"',
            'echo "cmd6"',
            'echo "cmd7"',
            'echo "cmd8"',
            'echo "cmd9"',
            'echo "cmd10"',
            'echo "cmd11"'
        ]
        results, failures = create_multiprocess_pool(
            shared_data={'log_timestamp': datetime.now().isoformat().__str__()},
            command_list=bash_commands,
            sleep_time=0,
            max_parallel_jobs=6
        )

        bash_commands.append('spark-submit')
        _results, _failures = create_multiprocess_pool(
            shared_data={'log_timestamp': datetime.now().isoformat().__str__()},
            command_list=bash_commands,
            sleep_time=0,
            max_parallel_jobs=6
        )

        self.assertEqual(len(failures), 0)
        self.assertEqual(len(_failures) > 0, True)

    def test_execute_bash(self):
        pid, return_code, yarn_application_id, stdout, stderr = \
            execute_bash(shared_data={'log_timestamp': datetime.now().isoformat().__str__()},
                         sleep_time=0, cmd='hadoop version')

        _pid, _return_code, _yarn_application_id, _stdout, _stderr = \
            execute_bash(shared_data={'log_timestamp': datetime.now().isoformat().__str__()},
                         sleep_time=0, cmd='spark-submit')
        self.assertNotEqual(pid, None)
        self.assertNotEqual(stderr, None)
        self.assertNotEqual(stdout, None)

        self.assertEqual(len(yarn_application_id), 0)
        self.assertEqual(return_code == 0, True)

        self.assertNotEqual(_pid, None)
        self.assertNotEqual(_stderr, None)
        self.assertNotEqual(_stdout, None)

        self.assertEqual(len(_yarn_application_id), 0)
        self.assertEqual(_return_code > 0, True)


if __name__ == '__main__':
    unittest.main()
