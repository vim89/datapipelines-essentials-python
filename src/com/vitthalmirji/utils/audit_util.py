import logging


def audit_action(action):
    def audit_decorator(func):
        def audit(*args, **kwargs):
            # Invoke the wrapped function first
            retval = func(*args, **kwargs)
            # Now do something here with retval and/or action
            logging.debug(f'Executed {action}, Callback return value {retval}')
            return retval
        return audit
    return audit_decorator
