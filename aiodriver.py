#!/usr/bin/python3
__author__ = 'ziyan.yin'

import logging
from urllib import parse

from .driver import sql_params


class AsyncDriver:

    def __init__(
            self,
            host: str = None,
            port: int = None,
            user: str = None,
            password: str = None,
            database: str = None,
            charset: str = 'utf8',
            **kwargs
    ):
        self._pool = None

        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset,
            'ignore_transactions': False
        }
        self.config.update(kwargs)
        self.logger = logging.getLogger('aio-db')

    def create_pool(self, **kwargs):
        raise NotImplementedError()

    async def initial(self):
        self._pool = await self.create_pool(**self.config)

    def acquire(self):
        return self._pool.acquire()

    def release(self, conn):
        return self._pool.release(conn)

    async def load_context(self):
        conn = await self.acquire()
        if not conn:
            raise ConnectionError('connection established error')

        def release():
            self.unload_context(conn)

        conn.release = release
        return conn

    def unload_context(self, conn):
        if not conn.closed:
            self.release(conn)

    @property
    async def cursor(self):
        class _Cursor:
            def __init__(self, connection, transaction, ignore_transactions):
                self.connection = connection
                self.cursor = connection.cursor()
                self.transaction = transaction
                self._transaction = None
                self.ignore_transactions = ignore_transactions

            async def __aenter__(self):
                cursor = await self.cursor.__aenter__()
                self._transaction = self.transaction(cursor)
                await self.begin()
                return cursor

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                await self.cursor.__aexit__(exc_type, exc_val, exc_tb)
                if exc_type is not None:
                    await self.rollback()
                else:
                    await self.commit()
                self.connection.release()

            async def commit(self):
                if hasattr(self._transaction, 'commit') and not self.ignore_transactions:
                    await self._transaction.commit()

            async def rollback(self):
                if hasattr(self._transaction, 'rollback') and not self.ignore_transactions:
                    await self._transaction.rollback()

            async def begin(self):
                if hasattr(self._transaction, 'begin') and not self.ignore_transactions:
                    await self._transaction.begin()

        conn = await self.load_context()
        return _Cursor(
            conn,
            transaction=lambda x: self.transaction(x),
            ignore_transactions=self.config['ignore_transactions']
        )

    def transaction(self, cursor):
        raise NotImplementedError()

    async def __cursor_wrapper(self, cursor, callback):
        if cursor:
            return await callback(cursor)
        else:
            async with await self.cursor as cursor:
                return await callback(cursor)

    async def execute(self, sql: str, params=None, cursor=None) -> int:
        if params is None:
            params = []
        sql = sql_params(sql, *params)
        try:
            if cursor:
                return await cursor.execute(sql)
            else:
                async with await self.cursor as cursor:
                    return await cursor.execute(sql)
        except Exception:
            self.logger.error(f'ERR: {sql}')
            raise

    async def query(self, sql: str, params=None, cursor=None, _test=False) -> list:
        if params is None:
            params = []
        if _test:
            return sql_params(sql, *params)

        async def _inner(transaction):
            await self.execute(sql, params, transaction)
            data = [x for x in await transaction.fetchall()]
            desc = transaction.description
            return desc, data

        description, rows = await self.__cursor_wrapper(cursor, _inner)
        if description:
            json_row = []
            if not rows:
                return json_row
            cols = [x[0] for x in description]
            for row in rows:
                obj = {}
                for prop, val in zip(cols, row):
                    obj[prop] = val
                json_row.append(obj)
            return json_row
        else:
            return rows

    async def insert(self, table: str, cursor=None, _last: str = '', _seq: str = None, _test=False, **values):
        columns = [x for x in values.keys()]
        if len(columns) > 0:
            sql = f"insert into {table} ({','.join([self.column_format(x) for x in columns])}) " \
                  f"values ({','.join(['{}'] * len(columns))}) {_last}"

            if _seq is not None:
                sql = self._process_insert_query(sql, table, _seq)

            if _test:
                return sql

            async def _inner(transaction):
                if isinstance(sql, tuple):
                    q1, q2 = sql
                    await self.execute(q1, [values[x] for x in columns], cursor=transaction)
                    await self.execute(q2, cursor=transaction)
                else:
                    await self.execute(sql, [values[x] for x in columns], cursor=transaction)
                try:
                    out = await transaction.fetchone()[0]
                except TypeError:
                    out = None
                return out

            return await self.__cursor_wrapper(cursor, _inner)
        else:
            return -1

    async def update(self, table: str, where: str = '', cursor=None, _test=False, **values):
        columns = [f'{k} = {{}}' for k in values.keys()]
        if len(columns) > 0:
            sql = f"update {table} set {','.join(columns)} where {where if where else '1=1'}"
            if _test:
                return sql
            return await self.execute(sql, [values[k] for k in values.keys()], cursor=cursor)
        else:
            return 0

    async def delete(self, table: str, where: str = '', cursor=None, _test=False):
        sql = f"delete from {table} where {where if where else '1=1'}"
        if _test:
            return sql
        return await self.execute(sql, cursor=cursor)

    async def insert_many(self, table: str, _last: str = '', cursor=None, _test=False, rows: list = None):

        def value_format(*args):
            return f"({sql_params(','.join(['{}'] * len(args)), *args)})"

        if rows and len(rows) > 0:
            if len(rows) > 0:
                try:
                    columns = [x for x in rows[0].keys()]
                    sql = f"insert into {table} ({','.join([self.column_format(x) for x in columns])}) values " \
                          f"{','.join([value_format(*[r[x] for x in columns]) for r in rows])} {_last}"
                    if _test:
                        return sql
                    return await self.execute(sql, cursor=cursor)
                except TypeError:
                    raise ValueError('object structure format error')
            else:
                return await self.insert(table, _last=_last, cursor=cursor, _test=_test, **rows[0])
        else:
            return 0

    def _process_insert_query(self, sql, seq_name, table_name):
        return sql + ";SELECT MAX({}) FROM {}".format(seq_name, table_name)

    @staticmethod
    def column_format(v):
        return f'`{v}`'


class AsyncNoSQLDriver:
    def __init__(
            self,
            host: str = None,
            port: int = None,
            user: str = None,
            password: str = None,
            database: str = None,
            charset: str = 'utf8',
            **kwargs
    ):
        self._pool = None
        self._db = None

        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset,
            'max_size': kwargs['max_size'] if 'max_size' in kwargs else 10
        }
        self.config.update(kwargs)
        self._client = None
        self.database = None
        self.logger = logging.getLogger('aio-nosql-db')

    def create_pool(self, **kwargs):
        raise NotImplementedError()


try:
    import aiomysql

    class AioMySQL(AsyncDriver):

        def create_pool(self, **kwargs):
            config = {
                'host': kwargs['host'],
                'port': kwargs['port'],
                'user': kwargs['user'],
                'password': kwargs['password'],
                'db': kwargs['database'],
                'charset': kwargs['charset']
            }
            return aiomysql.create_pool(**config)

        def _process_insert_query(self, query, seq_name, table_name):
            return query, 'SELECT last_insert_id();'

        def transaction(self, cursor):
            if 'isolation_level' in self.config:
                raise aiomysql.ProgrammingError('mysql cannot support isolation_level')
            return cursor.connection

except ImportError:
    aiomysql = AioMySQL = None

try:
    import aiopg

    class AioPostgreSQL(AsyncDriver):

        def create_pool(self, **kwargs):
            dsn = "dbname={0} user={1} password={2} host={3} port={4}".format(
                kwargs['database'], kwargs['user'], kwargs['password'], kwargs['host'], kwargs['port']
            )
            self.config['ignore_transactions'] = True
            return aiopg.create_pool(dsn)

        def _process_insert_query(self, sql, seq_name, table_name):
            if seq_name is None:
                seq_name = seq_name + "_id_seq"
                if seq_name not in self._get_all_sequences():
                    seq_name = None

            if seq_name:
                sql += "; SELECT currval('{}')".format(seq_name)

            return sql

        def _get_all_sequences(self):
            if self._sequences is None:
                q = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S'"
                self._sequences = set([c['relname'] for c in self.query(q)])
            return self._sequences

        @staticmethod
        def column_format(v):
            return f'"{v}"'

        def transaction(self, cursor):
            if 'isolation_level' in self.config:
                return aiopg.Transaction(cursor, aiopg.IsolationLevel[self.config['isolation_level']])
            return aiopg.Transaction(cursor, aiopg.IsolationLevel.default)

except ImportError:
    aiopg = AioPostgreSQL = None


try:
    import aiomongo

    class AioMongoDB(AsyncNoSQLDriver):

        async def create_pool(self, **keywords):
            auth = f"{parse.quote(keywords['user'])}:{parse.quote(keywords['password'])}@" \
                if keywords['user'] and keywords['password'] else ''
            uri = f"mongodb://{auth}{keywords['host']}:{keywords['port']}" \
                  f"/{keywords['database']}?maxpoolsize={keywords['max_size']}"
            self._client = await aiomongo.create_client(uri)
            self.database = self._client.get_database(keywords['database'])

except ImportError:
    aiomongo = AioMongoDB = None
