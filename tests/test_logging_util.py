import datetime
import unittest
import logging.config

from utils.Utilities import init_logging
from utils.audit_util import audit_action


class TestLoggingUtil(unittest.TestCase):
    def test_init_logging(self):
        init_logging(log_time_stamp=datetime.datetime.now())
        level20 = logging.getLogger('simpleExample').level
        self.assertEqual(level20, 20)

    def test_audit_action(self):
        @audit_action(action=f"testing Audit Action Wrapper")
        def audit_decorator():
            pass


if __name__ == '__main__':
    unittest.main()
