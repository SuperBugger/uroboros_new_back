import os
from abc import ABC, abstractmethod
from datetime import datetime

from .table import Table


def time_decorator(function_to_decorate):
    def the_wrapper_around_the_original_function(*args, **kwargs):
        print(datetime.now(), f"while {function_to_decorate.__name__}")
        return function_to_decorate(*args, **kwargs)

    return the_wrapper_around_the_original_function


class UploaderError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return 'UploaderError, this error is found while {} '.format(self.message)
        else:
            return 'UploaderError'


class BaseUploader(ABC):
    def __init__(self):
        super().__init__()
        self._db_helper = None
        self._uptables = dict()
        self._tmp_schema = "repositories"
        self._fk_tbl = []

    @abstractmethod
    def run(self, args):
        pass

    def _error(self, message):
        raise UploaderError(message)

    def fk_sort(self):
        for n, t in self._uptables.items():
            self._fk_tbl.append({t.name: t.tbl_id})
        for n, t in self._uptables.items():
            if len(t.fk_tbl) == 0:
                tbl = self._fk_tbl[self._fk_tbl.index({t.name: t.tbl_id})]
                self._fk_tbl.remove(tbl)
                self._fk_tbl.insert(0, tbl)
            else:
                for elem in self._fk_tbl:
                    for el in elem.values():
                        if el in t.fk_tbl:
                            tbl = self._fk_tbl[self._fk_tbl.index({t.name: t.tbl_id})]
                            self._fk_tbl.remove(tbl)
                            self._fk_tbl.insert(self._fk_tbl.index(elem)+1, tbl)

    def _ensure_uptable(self, name, fields, tbl_id, fk_tbl):
        upt = self._uptables.get(name, None)
        if not upt:
            upt = Table(self._db_helper, name, fields, tbl_id, fk_tbl)
            self._uptables[name] = upt
        return upt

    @time_decorator
    def _upload_tables(self):
        self.fk_sort()
        for tbl in self._fk_tbl:
            print(f" {self._uptables[list(tbl.keys())[0]].name}")
            self._uptables[list(tbl.keys())[0]].upload()
        sql = "call maintenance.update_rep_pkg();"
        self._db_helper.query(sql)
        self._db_helper.commit_conn()
