import logging

def create_console_handler(logger, log_format):
    c_handler = logging.StreamHandler()
    c_log_level = logging.INFO
    c_handler.setLevel(c_log_level)

    c_formater = logging.Formatter(log_format)
    c_handler.setFormatter(c_formater)

    return logger.addHandler(c_handler)

def create_file_handler(logger, file_name, log_format):
    f_handler = logging.FileHandler(file_name)
    f_log_level = logging.INFO
    f_handler.setLevel(f_log_level)

    f_formater = logging.Formatter(log_format)
    f_handler.setFormatter(f_formater)
    return logger.addHandler(f_handler)

log_format = '%(name)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s'
logger = logging.getLogger("first_logger")
logger.setLevel("INFO")

create_console_handler(logger, log_format)
create_file_handler(logger, "app_logs.log", log_format)

