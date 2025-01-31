import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)


def time_decorator(function_to_decorate):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        logging.debug(f"{datetime.now()} while {function_to_decorate.__name__}")
        return function_to_decorate(*args, **kwargs)

    return the_wrapper_around_the_original_function


class QueryError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return f'QueryError, this error is found while {self.message} '
        else:
            return 'QueryError'


class BaseApi(ABC):
    def __init__(self):
        super().__init__()
        self.tbl_dict = dict()
        self._db_helper = None
        self.table_name = ""
        self.fields = ""
        self.name_col = ""
        self.where = ""
        self.join = ""
        self.query = ""

    @time_decorator
    def create_query(self, tbl_id):
        pass

    def _error(self, message):
        raise QueryError(message)

    @time_decorator
    def run_query(self, build=True, t_id=True):
        try:
            if build:
                self.query = f"select {self.fields} from {self.table_name}"
                if self.join:
                    self.query += self.join
                if self.where:
                    self.query += f" where {self.where}"
            logging.debug(f"Executing query: {self.query}")
            table_info = self._db_helper.query(self.query)
            count = 0
            if table_info:
                for sttr in table_info:
                    if not t_id:
                        count += 1
                        self.tbl_dict[count] = {}
                        for i in range(len(self.name_col)):
                            self.tbl_dict[count][self.name_col[i]] = str(sttr[i])
                    else:
                        self.tbl_dict[sttr[0]] = {}
                        for i in range(1, len(self.name_col)):
                            self.tbl_dict[sttr[0]][self.name_col[i]] = str(sttr[i])
        except Exception as e:
            logging.error(f"Error while running query: {e}")
            self._error(e)

    @abstractmethod
    def run(self, args):
        pass
