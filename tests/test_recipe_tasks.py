import shutil
import unittest
from pathlib import Path

from com.hellofresh.datapipelines.recipe_tasks import main, task1, task2, determine_cooking_difficulty, \
    calculate_time_duration_average, standardize_and_rename_df_columns
from com.hellofresh.utils.data_quality import DataQuality
from com.hellofresh.utils.helpers import get_project_root, read_json_get_dict, convert_iso_to_time_duration
from com.hellofresh.utils.spark import get_or_create_spark_session


def del_dirs():
    try:
        shutil.rmtree(f"{get_project_root()}/resources/data-quality-reports/recipe-tasks")
        shutil.rmtree(f"{get_project_root()}/resources/data/output/task1")
        shutil.rmtree(f"{get_project_root()}/resources/data/output/task2")
    except:
        pass


class RecipeTasksTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.args = {
            'input_data_dir': f"{get_project_root()}/resources/data/input",
            'output_data_dir': f"{get_project_root()}/resources/data/output"
        }
        del_dirs()

    @unittest.skip
    def test_main(self):
        del_dirs()
        t1_dq = f"{get_project_root()}/conf/data-quality/rules/unit_test_configs/recipe-task1-dq-rules.json"
        t2_dq = f"{get_project_root()}/conf/data-quality/rules/unit_test_configs/recipe-task2-dq-rules.json"
        main(self.args, t1_dq, t2_dq)
        self.assertTrue(
            Path(f"{get_project_root()}/resources/data-quality-reports/recipe-tasks/task1-dq-report.html").is_file())

    def test_task1(self):
        del_dirs()
        t1_dq = f"{get_project_root()}/conf/data-quality/rules/unit_test_configs/recipe-task1-dq-rules.json"
        dq = read_json_get_dict(json_path=t1_dq)
        dq['execution_reports_dir'] = f"{get_project_root()}/resources/data-quality-reports/recipe-tasks"
        dq_rules = DataQuality(**dq)
        task1(input_data_path=self.args['input_data_dir'], input_file_type='json', dq_rules=dq_rules,
              output_data_path=f"{self.args['output_data_dir']}/task1", spark_opts={'encoding': 'utf-8'})

        self.spark = get_or_create_spark_session()
        df = self.spark.read.parquet(f"{self.args['output_data_dir']}/task1")
        self.assertEqual(df.count(), 1042)
        self.assertTrue(df.columns.__contains__('cook_time'))
        self.assertTrue(
            Path(f"{get_project_root()}/resources/data-quality-reports/recipe-tasks/task1-dq-report.html").is_file())

    def test_task2(self):
        t2_dq = f"{get_project_root()}/conf/data-quality/rules/unit_test_configs/recipe-task2-dq-rules.json"
        dq = read_json_get_dict(json_path=t2_dq)
        dq['execution_reports_dir'] = f"{get_project_root()}/resources/data-quality-reports/recipe-tasks"
        dq_rules = DataQuality(**dq)
        task2(input_data_path=f"{self.args['output_data_dir']}/task1", input_file_type='parquet', dq_rules=dq_rules,
              output_data_path=f"{self.args['output_data_dir']}/task2")

        self.spark = get_or_create_spark_session()
        df = self.spark.read.csv(f"{self.args['output_data_dir']}/task2", header=True)
        self.assertEqual(df.count(), 3)
        self.assertTrue(df.columns.__contains__('avg_total_cooking_time'))
        self.assertTrue(
            Path(f"{get_project_root()}/resources/data-quality-reports/recipe-tasks/task2-dq-report.html").is_file())

    def test_determine_cooking_difficulty(self):
        difficulty = determine_cooking_difficulty("PT1H", "PT2M")
        self.assertEqual(difficulty, ('PT1H2M', 'hard'))
        difficulty = determine_cooking_difficulty("PT5M", "PT15M")
        self.assertEqual(difficulty, ('PT20M', 'easy'))
        difficulty = determine_cooking_difficulty("PT15M", "PT20M")
        self.assertEqual(difficulty, ('PT35M', 'medium'))
        difficulty = determine_cooking_difficulty("PT", "PT")
        self.assertEqual(difficulty, ('P0D', 'easy'))

        try:
            difficulty = determine_cooking_difficulty("", "PT1H")
            print(difficulty)
        except Exception as ex:
            self.assertEqual(ex.__str__(), 'Expecting a string None')

    def test_calculate_time_duration_average(self):
        list_of_time_duration = list(map(lambda t: convert_iso_to_time_duration(t), ["PT1H", "PT30M", "PT", "PT2H5M"]))
        avg = calculate_time_duration_average(list_of_time_duration)
        self.assertEqual(avg, 'PT53M45S')


if __name__ == '__main__':
    unittest.main()
