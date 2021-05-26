#!/usr/bin/python3
__author__ = 'ziyan.yin'

import logging
from typing import Optional, Awaitable

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
        self._db = None

        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset,
            'ignore_nested_transactions': True
        }
        self.config.update(kwargs)
        self.commit = None
        self.rollback = None
        self.logger = logging.getLogger('aio-db')

    def create_pool(self, **kwargs):
        raise NotImplementedError()

    async def initial(self):
        self._pool = await self.create_pool(**self.config)

    def acquire(self) -> Optional[Awaitable[None]]:
        return self._pool.acquire()

    def release(self) -> Optional[Awaitable[None]]:
        return self._pool.release(self._db)

    async def load_context(self):

        if not self._db:
            self._db = await self.acquire()

        if not hasattr(self._db, 'commit'):
            self._db.commit = lambda: None

        if not hasattr(self._db, 'rollback'):
            self._db.rollback = lambda: None

        async def commit(unload=True):
            # do db commit and release the connection if pooling is enabled.
            await self._db.commit()
            if unload:
                await self.unload_context()

        async def rollback():
            # do db rollback and release the connection if pooling is enabled.
            await self._db.rollback()
            await self.unload_context()

        self.commit = commit
        self.rollback = rollback

    async def unload_context(self):
        await self.release()
        self._db = None

    @property
    async def cursor(self):
        if not self._db:
            await self.load_context()
        return self._db.cursor()

    async def execute(self, sql: str, params=None, cursor=None) -> int:
        if params is None:
            params = []
        sql = sql_params(sql, *params)
        try:
            if cursor:
                return await cursor.execute(sql)
            else:
                async with await self.cursor as cursor:
                    c = await cursor.execute(sql)
                    await self.commit(unload=True)
                    return c
        except Exception:
            self.logger.error(f'ERR: {sql}')
            await self.rollback()
            raise

    async def query(self, sql: str, params=None, _test=False) -> list:
        if params is None:
            params = []
        if _test:
            return sql_params(sql, *params)

        async with await self.cursor as cursor:
            await self.execute(sql, params, cursor)

            rows = [x for x in await cursor.fetchall()]
            await self.commit(unload=True)

            if cursor.description:
                json_row = []
                if not rows:
                    return json_row
                cols = [x[0] for x in cursor.description]
                for row in rows:
                    obj = {}
                    for prop, val in zip(cols, row):
                        obj[prop] = val
                    json_row.append(obj)
                return json_row
            else:
                return rows

    async def insert(self, table: str, _last: str = '', _seq: str = None, _test=False, **values):
        def column_format(v):
            return f'`{v}`'

        columns = [x for x in values.keys()]
        if len(columns) > 0:
            sql = f"insert into {table} ({','.join([column_format(x) for x in columns])}) " \
                  f"values ({','.join(['{}'] * len(columns))}) {_last}"

            if _seq is not None:
                sql = self._process_insert_query(sql, table, _seq)

            if _test:
                return sql
            async with await self.cursor as cursor:
                if isinstance(sql, tuple):
                    q1, q2 = sql
                    await self.execute(q1, [values[x] for x in columns], cursor)
                    await self.execute(q2, cursor=cursor)
                else:
                    await self.execute(sql, [values[x] for x in columns], cursor)
                try:
                    out = await cursor.fetchone()[0]
                except TypeError:
                    out = None
                await self.commit()
                return out
        else:
            return -1

    async def update(self, table: str, where: str = '', _test=False, **values):
        columns = [f'{k} = {{}}' for k in values.keys()]
        if len(columns) > 0:
            sql = f"update {table} set {','.join(columns)} where {where if where else '1=1'}"
            if _test:
                return sql
            return await self.execute(sql, [values[k] for k in values.keys()])
        else:
            return 0

    async def delete(self, table: str, where: str = '', _test=False):
        sql = f"delete from {table} where {where if where else '1=1'}"
        if _test:
            return sql
        return await self.execute(sql)

    async def insert_many(self, table: str, _last: str = '', _test=False, rows: list = None):

        def column_format(v):
            return f'`{v}`'

        def value_format(*args):
            return f"({sql_params(','.join(['{}'] * len(args)), *args)})"

        if rows and len(rows) > 0:
            if len(rows) > 0:
                try:
                    columns = [x for x in rows[0].keys()]
                    sql = f"insert into {table} ({','.join([column_format(x) for x in columns])}) values " \
                          f"{','.join([value_format(*[r[x] for x in columns]) for r in rows])} {_last}"
                    if _test:
                        return sql
                    return await self.execute(sql)
                except TypeError:
                    raise ValueError('object structure format error')
            else:
                return await self.insert(table, _last, _test=_test, **rows[0])
        else:
            return 0

    def _process_insert_query(self, sql, seq_name, table_name):
        return sql + ";SELECT MAX({}) FROM {}".format(seq_name, table_name)


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

except ImportError:
    aiomysql = AioMySQL = None
