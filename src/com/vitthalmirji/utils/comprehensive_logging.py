import logging
import logging.config
import sys
from pathlib import Path

from com.vitthalmirji.utils.constants import JOB_START_TIME
from com.vitthalmirji.utils.helpers import read_json_get_dict, get_project_root


def init_logging(job_name, log_time_stamp=JOB_START_TIME, log_path=f'{get_project_root()}/logs/python',
                 log_properties_path=f"{get_project_root()}/conf/python/logging-properties.json"):
    """
    Initiates the logging object with given configurations

    Args:
        :param log_properties_path: Location of properties file.
                                    default to local project folder's <project-root>/conf/python/logging-properties.json
        :param job_name: Name of the application
        :param log_time_stamp: Timestamp to append in log file name
        :param log_path: Location to store logs.
                         Default location <project-root>/logs/python/

    Returns: N/A
    """
    Path(log_path).mkdir(parents=True, exist_ok=True)
    log_conf = read_json_get_dict(json_path=log_properties_path)
    log_file = f"{log_path}/log-{job_name}_{log_time_stamp}.log"
    log_conf['handlers']['file']['filename'] = log_file

    # In case of Unit test cases do not log to file
    if 'unittest' in sys.modules.keys():
        log_conf['handlers'] = {'console': log_conf['handlers']['console']}
        log_conf['root']['handlers'] = ['console']

    print('Logging initiating using below properties')
    print(log_conf)
    logging.config.dictConfig(log_conf)
    logging.info(f'Logging initiated; appending logs to {log_file}')
