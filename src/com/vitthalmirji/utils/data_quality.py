import datetime
import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import count

from com.vitthalmirji.utils.helpers import create_dir, log_exception_details
from com.vitthalmirji.utils.spark import get_or_create_spark_session


class Rule(object):
    def __init__(self, rule_id: int, name: str, description: str, rule_type: str, columns: List[str] = None,
                 query: str = None):
        self.rule_id = rule_id
        self.name = name
        self.description = description
        self.rule_type = rule_type
        self.columns = columns if columns else None
        self.query = query if query else None


class RuleExecutionResult:
    def __init__(self, rule: Rule, status, pass_count: int, fail_count: int, total_count):
        self.rule = rule
        self.status = status
        self.pass_count = pass_count
        self.fail_count = fail_count
        self.total_count = total_count


class DataQuality(object):
    def __init__(self, dq_id: int, rules: list[dict] = None, email_execution_report_to: str = None,
                 execution_reports_dir: str = None):
        logging.info(
            f"Initializing Data quality service for DQ ID {dq_id}, reports will be available in file {execution_reports_dir}")
        self.html_report = None
        self.df = None
        self.total_count = None
        self.execution_results = None
        self.dq_id = dq_id
        self.rules: List[Rule] = [Rule(**rule) for rule in rules] if rules else None
        self.email_execution_report_to = email_execution_report_to if email_execution_report_to else None
        self.spark = get_or_create_spark_session()
        self.yarn_id = self.spark.sparkContext.applicationId
        self.execution_reports_dir = execution_reports_dir if execution_reports_dir else None
        if self.execution_reports_dir:
            create_dir(self.execution_reports_dir)

    def execute_unique_rule(self, rule: Rule):
        """
        Executes Duplicates check on given Primary keys in `rule`

        Args:
            :param rule: Rule of type `unique` having list of primary keys

        Returns:
            :return: RuleExecutionResult with status fail if duplicates are present pass otherwise and count of duplicates

        Exceptions:
            :exception: Thrown by calling functions called in this function
        """
        logging.warning(f"Executing DQ Rule for {rule.name} on {rule.columns}")
        dups_count = self.df.select(rule.columns).groupby(rule.columns).agg(count("*").alias('cnt')).alias(
            'cnt').filter('cnt > 1').count()

        return RuleExecutionResult(rule, 'fail' if dups_count > 0 else 'pass', self.total_count - dups_count,
                                   dups_count, self.total_count)

    def execute_not_null_rule(self, rule: Rule):
        """
        Executes Not null check on given list of columns in `rule`

        Args:
            :param rule: Rule of type `not null` having list of columns potentially not null

        Returns:
            :return: RuleExecutionResult with status fail if column values are null & pass otherwise and count of null records

        Exceptions:
            :exception: Thrown by calling functions called in this function
        """
        logging.warning(f"Executing DQ Rule for {rule.name} on {rule.columns}")
        filter_string = ' OR '.join(list(map(lambda c: f'{c} IS NULL OR TRIM({c}) = ""', rule.columns)))
        not_null_count = self.df.select(rule.columns).filter(filter_string).count()
        return RuleExecutionResult(rule, 'fail' if not_null_count > 0 else 'pass', self.total_count - not_null_count,
                                   not_null_count, self.total_count)

    def execute_query_rule(self, rule: Rule):
        """
        Executes query given in `rule`
        This is in case of custom data quality rule given in form of query
        Args:
            :param rule: Rule of type `query` having query to execute

        Returns:
            :return: RuleExecutionResult with status fail if duplicates are present pass otherwise and count of duplicates

        Exceptions:
            :exception: Thrown by calling functions called in this function
        """
        self.df.createOrReplaceTempView('temp')
        query = rule.query
        logging.warning(f"Executing DQ Rule for {rule.name} using query {rule.query}")
        query_count = self.spark.sql(query).count()
        return RuleExecutionResult(rule, 'fail' if query_count > 0 else 'pass',
                                   self.total_count - query_count,
                                   query_count, self.total_count)

    def execute_rules(self, df: DataFrame) -> tuple[bool, str]:
        """
        Executes list of rules (data quality checks) given on dataframe's data
        Args:
            :param df: Dataframe on which quality checks to be executed
            :param rules: List of rules mapped to Rule type

        Returns:
            :return: boolean status True if all rules executed successfully without any failures, False otherwise and
                     HTML report of details executed rules

        Exceptions:
            :exception: All exceptions thrown by calling functions called in this function
        """
        logging.info("Starting data quality rules executions..")
        self.execution_results: List[RuleExecutionResult] = []
        self.df = df
        self.total_count = self.df.count()
        for unique_rule in list(filter(lambda r: r.rule_type.__eq__('unique'), self.rules)):
            self.execution_results.append(self.execute_unique_rule(unique_rule))

        for not_null_rule in list(filter(lambda r: r.rule_type.__eq__('not null'), self.rules)):
            self.execution_results.append(self.execute_not_null_rule(not_null_rule))

        for query_rule in list(filter(lambda r: r.rule_type.__eq__('query'), self.rules)):
            self.execution_results.append(self.execute_query_rule(query_rule))

        return False if list(filter(lambda exec_result: exec_result.status.__eq__('fail'), self.execution_results)) \
            else True, self.generate_report()

    def generate_report(self):
        """
        Generates HTML report of result of executed data quality checks

        Args: N/A

        Returns:
            :return: self.html_report a HTML report of details about executed DQ checks

        Exceptions:
            :exception: All exception thrown by calling functions called in this function
        """
        logging.info(f"Preparing Data quality rules report for {self.dq_id}")
        table_header = ' '.join(list(
            map(lambda header: f"<th>{header}</th>", ["Yarn Application Id", "DQ ID", "Rule ID", "Rule Name",
                                                      "Rule type", "Description", "Columns/Query", "Pass Count",
                                                      "Fail Count",
                                                      "Total Count"])))

        def rules_and_result(result: RuleExecutionResult):
            table_data = [self.yarn_id,
                          self.dq_id,
                          result.rule.rule_id,
                          result.rule.name,
                          result.rule.rule_type,
                          result.rule.description,
                          result.rule.columns,
                          result.pass_count,
                          result.fail_count,
                          result.total_count
                          ]
            return ' '.join(list(map(lambda d: f"<td>{d}</td>", table_data)))

        failed_rules = list(filter(lambda result: result.status.__eq__('fail'), self.execution_results))
        failed_details = ' '.join(list(map(lambda result: f"<tr>{rules_and_result(result)}</tr>", failed_rules)))
        failure_table = f'<h3 style="font-family:arial">Failed DQ details</h3>' \
                        f'<table border="3" style="width:100%"><tr style="text-align:left;background-color:#FF6347">{table_header}</tr>' \
                        f'{failed_details}</table>' if failed_rules else ""

        passed_rules = list(filter(lambda result: result.status.__eq__('pass'), self.execution_results))
        passed_details = ' '.join(list(map(lambda result: f"<tr>{rules_and_result(result)}</tr>", passed_rules)))
        passed_table = f'<h3 style="font-family:arial">Succeeded DQ details</h3>' \
                       f'<table border="3" style="width:100%"><tr style="text-align:left;background-color:#33FFBD">{table_header}</tr>' \
                       f'{passed_details}</table>' if passed_rules else ""

        opening_statement = "<html><body><P>Team,<br/><br/>" \
                            f"Data Quality check finished successfully for <b>DQ ID = {self.dq_id}</b>" \
                            f"{', with failures. ' if failed_rules else '. '}" \
                            "Check details in below table of metrics.</P>"
        closing_statement = "<br/><br/>" \
                            f"Executed on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},<br/>" \
                            "Thanks</body></html><br/>"

        self.html_report = f"{opening_statement}" \
                           f"{failure_table if failed_rules else ''}" \
                           f"{passed_table if passed_rules else ''}" \
                           f"{closing_statement}"

        return self.html_report

    def write_report_to_html(self, file_name):
        """
        Writes Data Quality rules execution results to a html file

        Args:
            :param file_name: name of file to write report as HTML file

        Returns:
            :return: N/A

        Exceptions:
            :exception Throws exception if unable to write into html file but will not halt the execution process
        """
        logging.info(f"Writing data quality execution report to html file {self.execution_reports_dir}/{file_name}")
        try:
            if not self.execution_reports_dir:
                raise Exception("Empty file path")
            f = open(self.execution_reports_dir + "/" + file_name, "w")
            f.write(self.html_report)
            f.close()
        except Exception as ex:
            log_exception_details(message="Error writing report to html, skipping writing report",
                                  exception_object=ex)
