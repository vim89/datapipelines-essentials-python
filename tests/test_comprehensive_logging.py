import logging
import unittest
import logging.config

from com.hellofresh.utils.comprehensive_logging import init_logging


class LoggingTestCases(unittest.TestCase):
    def test_init_logging(self):
        init_logging(job_name='Unit tests')
        logger = logging.getLogger('root')
        self.assertEqual(logger.level, 10)


if __name__ == '__main__':
    unittest.main()
