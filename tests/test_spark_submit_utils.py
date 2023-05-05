import datetime
import logging
import unittest

from utils.Utilities import init_logging, cast_string_to_date, get_project_root, read_json_get_dict, read_yaml_get_dict
from utils.spark_submit_utils import prepare_spark_submit


class TestSparkSubmitUtils(unittest.TestCase):
    init_logging(datetime.datetime.now())

    def test_get_project_root(self):
        self.assertEqual(get_project_root().__str__(), '/Users/v0m02sj/PycharmProjects/datapipelines-essentials')

    def test_cast_string_to_date(self):
        dt = cast_string_to_date('2020-01-01', '%Y-%m-%d')
        _dt = cast_string_to_date('abcdefg', '%Y-%m-%d')
        self.assertEqual(type(dt), datetime.datetime)
        self.assertEqual(_dt, None)

    def test_prepare_spark_submit_command(self):
        application_properties = read_json_get_dict(
            json_path=f"{get_project_root()}/main/src/resources/config/application_properties.json")
        runtime_args = {}  # parse_arguments(application_properties.get('command_line_args'))
        runtime_args.update({
            "workflow": "DVSkuDailyChannelWorkFlow",
            "refreshType": "history",
            "startDate": "2020-01-01",
            "endDate": "2020-01-10",
            "dq_enabled": "Y",
            "configFile": "/Users/v0m02sj/IdeaProjects/channel-perf-data-pipeline/configs/config-prod.yml"
        })
        static_args = read_yaml_get_dict(runtime_args['configFile'])
        runtime_args.update({'configFile': runtime_args['configFile'].split('/')[-1]})
        commands = prepare_spark_submit(runtime_args=runtime_args, config_args=static_args,
                                        app_config=application_properties)
        logging.debug(commands)
        self.assertEqual(len(commands) > 0, True)


if __name__ == '__main__':
    unittest.main()
