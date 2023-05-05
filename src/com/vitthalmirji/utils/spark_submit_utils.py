import logging
import logging.config
from datetime import datetime

from com.vitthalmirji.utils.Utilities import get_dates_between_range

START_TIME = datetime.now().isoformat().__str__()


class StaticConfigParameterNotFound(Exception):
    pass


def sort_spark_submit_options(command_options):
    sorted_command_options = sorted(command_options, key=lambda k: k[0])
    return sorted_command_options


def update_conf(command_options):
    _conf = {k: v for k, v in command_options.items() if k == '--conf'}['--conf']['value']
    _conf = get_spark_conf_key_value_as_string(_conf)
    _conf = f"\"{_conf}\""
    command_options['--conf'].update({'value': _conf})

    return command_options


def get_class_arguments_as_string(command):
    return ' \\\n'.join(list(map(lambda c: f"{c}={command['--class_arguments']['value'][c]}",
                                 command['--class_arguments']['value'])))


def get_spark_conf_key_value_as_string(conf):
    return f"""{",".join([f"{d}={conf[d]}" for d in conf])}\""""


def static_config_args_sanity_check(command, config):
    for cmd in command:
        if config.get(cmd) is None and command[cmd]['required'] is True:
            logging.error(f"Configuration file do not have required spark-submit option {cmd}")
            raise StaticConfigParameterNotFound(
                f"ERROR: Configuration file do not have required spark-submit option {cmd}")
        elif config.get(cmd) is not None:
            command[cmd].update({'value': config.get(cmd)})
        else:
            continue
    return command


def update_spark_submit_option_values(runtime_args, config_args, command):
    config_args['default']['--conf'].update(config_args[runtime_args['workflow']]['spark_conf'])
    config_args['default']['--name'] = f"\"{runtime_args['workflow']}\""
    command['--class_arguments']['value'].update(runtime_args)
    return config_args, command


def prepare_spark_submit(runtime_args, config_args, app_config):
    command = app_config['spark_submit_options_order']
    _config_args, command = update_spark_submit_option_values(runtime_args, config_args, command)
    _config_args = _config_args['default']

    command = static_config_args_sanity_check(command, _config_args)

    command_date_ranges = get_dates_between_range(refresh_type=runtime_args['refreshType'],
                                                  start_date=runtime_args['startDate'],
                                                  end_date=runtime_args['endDate'],
                                                  interval_in_days=app_config['default_settings'][
                                                      'history_load_interval_in_days'],
                                                  date_pattern='%Y-%m-%d')
    logging.debug(f"Date Range = {command_date_ranges}")
    command = update_conf(command_options=command)
    spark_submit_command = ' \\\n'.join(f"{k} {v['value']}" for k, v in command.items() if k != '--class_arguments')

    logging.debug(command)
    command_list = []
    for d in command_date_ranges:
        command['--class_arguments']['value'].update(d)
        class_args = get_class_arguments_as_string(command)
        command_list.append(f"{spark_submit_command}\n{class_args}")
    return command_list
