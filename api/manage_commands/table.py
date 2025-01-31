from datetime import datetime


class Table:
    def __init__(self, db_helper, name, fields, tbl_id, fk_tbl):
        self._db_helper = db_helper
        self.name_ = name
        self.key2id = {}
        self._rows = {}
        self.tbl_id = tbl_id
        self._fields = fields
        self.fk_tbl = fk_tbl
        self._stmt_name = None
        self._schm = None
        self.seq = 0
        self._prepare_stmt = None
        self._insert_stmt = None
        self.clear()
        self._init_id()

    @property
    def name(self):
        return self.name_

    @property
    def fields(self):
        return self._fields

    @property
    def rows(self):
        return self._rows

    def _prepare_insert(self):
        self._stmt_name = '{}_stmt'.format(self.name)
        self._prepare_stmt = '''
                PREPARE {} AS
                INSERT INTO "{}"."{}"({}) VALUES ({})
               '''.format(self._stmt_name, self._schm, self.name,
                          ', '.join(self._fields),
                          ', '.join('${}'.format(i) for i in range(1,
                                                                   len(self._fields) + 1))) + f"on conflict do nothing;"

        self._insert_stmt = '''
                EXECUTE {} ({})'''.format(self._stmt_name,
                                          ', '.join(['%s'] * len(self._fields)))

    def _internal_upload(self):
        for key, row in self._rows.items():
            self._db_helper.query([self._insert_stmt, row])
        self._db_helper.commit_conn()

    def upload(self, conflict_value=None):
        self._prepare_insert()
        self._db_helper.query(self._prepare_stmt)
        self._internal_upload()

    def data_exist(self, key):
        id = self.key2id.get(key, None)
        if not id:
            return False
        return True

    def clear(self):
        self._rows = {}

    def update(self, key, field, value):
        self._rows[key][field] = value

    def getid(self, key, sql_attr=None):
        id = self.key2id.get(key, None)
        sql = f"SELECT max({self.tbl_id}) from repositories.{self.name} where"
        if not id:
            if self.seq != 0:
                if sql_attr is not None:
                    if isinstance(key, str) or isinstance(key, datetime):
                        sql += f" {sql_attr}='{key}'"
                    if isinstance(key, int):
                        sql += f" {sql_attr}={key}"
                    if isinstance(key, tuple):
                        for attr in range(len(sql_attr)):
                            if isinstance(key[attr], str) or isinstance(key[attr], datetime):
                                if attr == 0:
                                    sql += f" {sql_attr[attr]}='{key[attr]}'"
                                else:
                                    sql += f" and {sql_attr[attr]}='{key[attr]}'"
                            else:
                                if attr == 0:
                                    sql += f" {sql_attr[attr]}={key[attr]}"
                                else:
                                    sql += f" and {sql_attr[attr]}={key[attr]}"
                    var = self._db_helper.query(sql)
                    if isinstance(var, list):
                        if var[0][0] is not None:
                            self.key2id[key] = var[0][0]
                            return var[0][0]
                    if isinstance(var, tuple):
                        if var[0] is not None:
                            self.key2id[key] = var[0]
                            return var[0]
            self.seq += 1
            self.key2id[key] = self.seq
            id = self.seq
        return id

    def _init_id(self):
        if not isinstance(self.tbl_id, list):
            sql = f"SELECT COUNT(*) FROM repositories.{self.name}"
            if self._db_helper.query(sql)[0][0] == 0:
                self.seq = 0
            else:
                sql = f"SELECT last_value FROM repositories.{self.name}_{self.tbl_id}_seq"
                self.seq = self._db_helper.query(sql)[0][0]

    def upsert(self, key, row):
        self._rows[key] = row
