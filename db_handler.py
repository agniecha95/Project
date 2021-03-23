import os
import sqlite3

from Connection_retry import connection_retry
from CustomLogger import logger

class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = self.connect(tries=3, delay=0.1, db_name=db_name)

    @staticmethod
    @connection_retry
    def connect():
        pass

    def execute_sql(self, sql, params=None):
        if params is None:
            params =[]
        #conn = self.connect()
        if self.conn:
            try:
                curr = self.conn.cursor()
                curr.execute(sql,params)
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error {e}")

    def close_conn(self):
        try:
            self.conn.close()
        except AttributeError as e:
            logger.error("No connection to close")

    def select_all_tasks(self, table_name):
        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute((f"SELECT * FROM {table_name}"))
            rows = cur.fetchall()
            return rows
        except Exception as e:
            logger.error(f"Can not fetch database {table_name} \nError {e}")