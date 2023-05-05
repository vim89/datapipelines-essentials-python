import getpass
import json
import logging
import traceback
from pathlib import Path

import isodate
from isodate import ISO8601Error


def create_dir(dir_path):
    """
    Creates directory from given path
    :param dir_path: relative path of directory to create
    :return: N/A
    """
    try:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    except Exception as ex:
        msg = f"Error creating directory from given relative path {dir_path}"
        log_exception_details(message=msg, exception_object=ex)
        raise ex


def get_user():
    """
    Fetches username of the executor

    Args:

    Returns:
        :return: username of the executor / logged in machine
    """
    return getpass.getuser()


def is_null_or_empty(obj) -> bool:
    """
    Checks if an object is null or empty if object is of type string

    Args:
        :param obj: object / variable to validate

    Returns:
        :return: bool True of object is null or string is empty False otherwise
    """
    if obj is None:
        return True
    elif type(obj) is str and str(obj).strip().__eq__(''):
        return True
    else:
        return False


def get_project_root() -> Path:
    """
    Identifies project root, Returns project root, the repository root
    Args:

    Returns:
        :return: project's root path as type Path
    """
    return Path(__file__).parent.parent.parent.parent.parent


def read_json_get_dict(json_path) -> dict:
    """
    Reads json file from given `json_path` & returns as python dict
    Args:
        :param :json_path : Absolute or Relative path of json file to read & convert

    Return:
        :return :json_as_dict: JSON content as dictionary type
    """
    try:
        with open(json_path, 'r') as stream:
            json_as_dict = json.load(stream)
        stream.close()
        return json_as_dict
    except Exception as ex:
        log_exception_details(f'Error reading json file {json_path}, error traceback below', ex)


def log_exception_details(message, exception_object):
    """
    Logs the exception to console & log file for every exception

    Args:
        :param message: Developer's message on exception
        :param exception_object: Class object of the exception

    Returns: N/A
    """
    logging.error(exception_object.__str__())
    logging.error(traceback.format_exc())
    logging.exception(message)


def convert_iso_to_time_duration(iso_time_duration: str):
    """
    Converts ISO time duration to time in hours, minutes & seconds

    Args:
        :param iso_time_duration: ISO time in string Example: PT1H, PT100M, PT2H5M

    Returns:
        :return: Returns duration as datetime.timedelta type.
                 Example: 01:00:00, 01:40:00, 02:05:00
    """
    if is_null_or_empty(iso_time_duration):
        msg = f'Empty or Invalid time duration string {iso_time_duration}'
        logging.error(msg)
        return None
    try:
        return isodate.parse_duration(iso_time_duration)
    except ISO8601Error as isoError:
        msg = f"Error converting ISO time {iso_time_duration} to timedelta"
        log_exception_details(message=msg, exception_object=isoError)
        return None


def add_iso_time_duration(time1: str, time2: str):
    """
    Adds two string time duration, first converts to timedelta then adds to return the result
    Args:
        :param time1: First time as string value
        :param time2: Second time as string value

    Returns:
        :return: time1 + time2 as datetime.timedelta type
    """
    if is_null_or_empty(time1) or is_null_or_empty(time2):
        msg = f'Empty or Invalid time duration string time1 = {time1}, time2 = {time2}'
        logging.error(msg)
        return None

    try:
        _time1 = convert_iso_to_time_duration(iso_time_duration=time1)
        _time2 = convert_iso_to_time_duration(iso_time_duration=time2)
        return isodate.duration_isoformat((_time1 + _time2))
    except ISO8601Error as isoError:
        msg = f"Error converting ISO time time1={time1} & time2={time2} to timedelta"
        logging.error(msg)
        log_exception_details(message=msg, exception_object=isoError)
        return None
