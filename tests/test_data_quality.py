import shutil
import unittest
from pathlib import Path

from com.hellofresh.utils.data_quality import Rule, RuleExecutionResult, DataQuality
from com.hellofresh.utils.helpers import get_project_root, read_json_get_dict
from com.hellofresh.utils.spark import get_or_create_spark_session


class DataQualityTestCases(unittest.TestCase):
    def test_Rule(self):
        rule_dict = {
            "rule_id": 1011,
            "name": "Primary / Natural Keys",
            "description": "Primary / Natural Keys should not have duplicates",
            "rule_type": "unique",
            "columns": [
                "name"
            ]
        }
        rule = Rule(**rule_dict)
        self.assertEqual(rule.rule_id, 1011)
        self.assertEqual(rule.name, "Primary / Natural Keys")

    def test_RuleExecutionResult(self):
        rule_dict = {
            "rule_id": 1011,
            "name": "Primary / Natural Keys",
            "description": "Primary / Natural Keys should not have duplicates",
            "rule_type": "unique",
            "columns": [
                "name"
            ]
        }
        rule = Rule(**rule_dict)
        result = RuleExecutionResult(rule, 'fail', 0, 0, 0)
        self.assertEqual(result.status, 'fail')
        self.assertEqual(result.rule, rule)
        self.assertEqual(result.rule.rule_type, 'unique')

    def test_data_quality(self):
        shutil.rmtree(f"{get_project_root()}/resources/data-quality-reports/recipe-tasks")
        t1_dq = f"{get_project_root()}/conf/data-quality/rules/unit_test_configs/recipe-task1-dq-rules.json"
        dq = read_json_get_dict(json_path=t1_dq)
        dq['execution_reports_dir'] = f"{get_project_root()}/resources/data-quality-reports/recipe-tasks"
        dq_rules = DataQuality(**dq)
        spark = get_or_create_spark_session()
        df = spark.read.option('encoding', 'utf-8').json(f"{get_project_root()}/resources/data/input")
        execution_result = dq_rules.execute_rules(df=df)
        self.assertEqual(execution_result[0], False)
        self.assertTrue(execution_result[1].__contains__('<html>'))
        dq_rules.write_report_to_html(file_name="task1-dq-report.html")
        self.assertTrue(
            Path(f"{get_project_root()}/resources/data-quality-reports/recipe-tasks/task1-dq-report.html").is_file())


if __name__ == '__main__':
    unittest.main()
