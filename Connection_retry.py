import os
import sqlite3
import time

from CustomLogger import logger

def connection_retry(fun):
    def wrapper(**kwargs):
        tries = kwargs['tries']
        delay = kwargs['delay']
        db_name = kwargs['db_name']

        for i in range(int(tries)):
            logger.info(f"It is my {i} try to connect")
            conn = None
            if os.path.isfile(db_name):
                try:
                    conn = sqlite3.connect(db_name)
                except Exception as e:
                    logger.error(f"The {i} attempt failed", exc_info=True)
                    time.sleep(delay)
                else:
                    logger.info("Connection established")
                return conn
            else:
                logger.error(f"No database {db_name}")
    return wrapper