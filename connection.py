import re
import psycopg2
import io
from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extras import DictCursor


class DbHelper(object):
    def __init__(self, db_name, user, password, host, port):
        self._connection = None
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._cursor = None
        self._db_name = db_name
        self.connect()

    def connect(self):
        try:
            self._connection = psycopg2.connect(
                database=self._db_name,
                user=self._user,
                password=self._password,
                host=self._host,
                port=self._port,
            )
            print("Connection to PostgreSQL DB successful")
            return self._connection
        except OperationalError as e:
            print(f"The error '{e}' occurred")
            return None

    def query(self, sql, params=None):
        self._cursor = self._connection.cursor()
        try:
            if params:
                self._cursor.execute(sql, params)
            else:
                self._cursor.execute(sql)

            if re.search(r"select", sql.lower()) and not sql.lower().startswith("create"):
                result = self._cursor.fetchall()
                return result
        except ProgrammingError as e:
            raise e
        finally:
            self._cursor.close()

    def execute_query(self, query, params=None):
        try:
            if params:
                return self.query(query, params)
            else:
                return self.query(query)
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def commit_conn(self, end_status=False):
        if self._cursor is not None:
            self._cursor.close()
        self._connection.commit()
        if end_status:
            self._connection.close()


# class DbHelper(object):
#     def __init__(self, db_name, user, password, host, port):
#         self._connection = None
#         self._user = user
#         self._password = password
#         self._host = host
#         self._port = port
#         self._db_name = db_name
#         self.connect()
#
#     def connect(self):
#         """Подключение к базе данных"""
#         try:
#             self._connection = psycopg2.connect(
#                 database=self._db_name,
#                 user=self._user,
#                 password=self._password,
#                 host=self._host,
#                 port=self._port,
#                 cursor_factory=DictCursor
#             )
#             self._connection.autocommit = False
#             print("Connection to PostgreSQL DB successful")
#         except psycopg2.OperationalError as e:
#             print(f"Database connection error: {e}")
#
#     def query(self, sql, params=None):
#         """Выполняет SQL-запрос и возвращает все результаты"""
#         with self._connection.cursor() as cursor:
#             try:
#                 cursor.execute(sql, params or ())
#                 if re.search(r"select", sql.lower()) and not sql.lower().startswith("create"):
#                     return cursor.fetchall()
#             except psycopg2.ProgrammingError as e:
#                 raise e
#
#     def execute_query(self, query, params=None):
#         """Обертка для выполнения SQL-запроса"""
#         try:
#             return self.query(query, params)
#         except Exception as e:
#             print(f"Error executing query: {e}")
#             return None
#
#     def execute_query_stream(self, query, params=None, fetch_size=1000):
#         """
#         Выполняет SQL-запрос и возвращает генератор, который читает данные порциями (по `fetch_size` строк).
#         """
#         try:
#             cursor = self._connection.cursor(name="stream_cursor")
#             self._connection.autocommit = False  # Начинаем транзакцию
#             cursor.itersize = fetch_size
#             cursor.execute(query, params or ())
#
#             while True:
#                 rows = cursor.fetchmany(fetch_size)
#                 if not rows:
#                     break
#                 for row in rows:
#                     yield dict(row)
#         except Exception as e:
#             print(f"❌ Error executing streaming query: {e}")
#         finally:
#             if cursor:
#                 cursor.close()
#             self._connection.commit()
#
#     def commit_conn(self, end_status=False):
#         """
#         Коммит транзакции и (по желанию) закрытие соединения.
#         Использовать `end_status=True`, если нужно завершить соединение.
#         """
#         try:
#             if self._connection:
#                 self._connection.commit()
#                 print("✅ Transaction committed")
#                 if end_status:
#                     self._connection.close()
#                     print("🔴 Connection closed")
#         except Exception as e:
#             print(f"❌ Error committing transaction: {e}")
