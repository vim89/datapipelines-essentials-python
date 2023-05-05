import datetime
import getpass
import unittest
from pathlib import Path

from isodate import ISO8601Error

from com.hellofresh.datapipelines.recipe_tasks import determine_cooking_difficulty
from com.hellofresh.utils.comprehensive_logging import init_logging
from com.hellofresh.utils.helpers import get_user, get_project_root, convert_iso_to_time_duration, \
    add_iso_time_duration


class UtilsHelpersTestCases(unittest.TestCase):
    init_logging(job_name='UtilsHelpersTestCases')

    def test_get_user(self):
        user = get_user()
        self.assertEqual(user, getpass.getuser())

    def test_get_project_root(self):
        project_root_path: Path = get_project_root()
        self.assertEqual(project_root_path.name, 'vim89-data-engineering-test')

    def test_convert_iso_to_time_duration(self):
        try:
            convert_iso_to_time_duration("")
        except ValueError as v:
            self.assertEqual(v.__str__(), 'Empty or Invalid time duration string')

        try:
            convert_iso_to_time_duration("ABC")
        except ISO8601Error as i:
            self.assertEqual(i.__str__(), 'Error converting ISO time ABC to timedelta')

        iso_time = convert_iso_to_time_duration("PT100M")
        self.assertEqual(iso_time, datetime.timedelta(hours=1, minutes=40))

        iso_time = convert_iso_to_time_duration("PT")
        self.assertEqual(iso_time, datetime.timedelta(0))

    def test_add_iso_time_duration(self):
        try:
            add_iso_time_duration(time1="", time2="PT1H")
        except ValueError as v:
            self.assertEqual(v.__str__(), 'Empty or Invalid time duration string')

        iso_time = add_iso_time_duration(time1="PT100M", time2="PT1H")
        self.assertEqual(iso_time, "PT2H40M")

        iso_time = add_iso_time_duration(time1="PT", time2="PT5M")
        self.assertEqual(iso_time, "PT5M")

        iso_time = add_iso_time_duration(time1="PT", time2="PT")
        self.assertEqual(iso_time, "P0D")

    def test_determine_difficulty(self):
        difficulty = determine_cooking_difficulty(cook_time="PT", prep_time="PT")
        self.assertEqual(difficulty, ('P0D', 'easy'))

        difficulty = determine_cooking_difficulty(cook_time="PT21H", prep_time="PT")
        self.assertEqual(difficulty, ('PT21H', 'hard'))

        difficulty = determine_cooking_difficulty(cook_time="PT", prep_time="PT100M")
        self.assertEqual(difficulty, ('PT1H40M', 'hard'))


if __name__ == '__main__':
    unittest.main()
