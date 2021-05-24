#!/usr/bin/python3
__author__ = 'ziyan.yin'

import datetime
import decimal
import logging

from store import Store

try:
    import dbutils
    from dbutils import pooled_db
    has_pooling = True
except ImportError:
    pooled_db = None
    has_pooling = False


def _sql_params(sql, *args):
    params = list()
    for p in args:
        if p is None:
            params.append('NULL')
        elif type(p) in (int, float, decimal.Decimal):
            params.append(str(p))
        elif type(p) is datetime.datetime:
            params.append(f"'{str(p)}'")
        else:
            params.append(f"""\'{str(p).replace("'", "''")}\'""")
    return sql % tuple(x for x in params)


def get_pooled_db(engine, **keywords):
    if pooled_db.__version__.split('.') < '0.9.3'.split('.'):
        return pooled_db.PooledDB(dbapi=engine, **keywords)
    else:
        return pooled_db.PooledDB(creator=engine, **keywords)


def _unload_context(ctx):
    del ctx.db


class Driver:

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
        self._ctx = Store()
        self._cursor = None
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
        self.module = None
        self.has_pooling = self.config.pop('pooling', True) and has_pooling

        self.logger = logging.getLogger('db')

    def _get_ctx(self):
        if not self._ctx.get('db'):
            self._load_context(self._ctx)
        return self._ctx

    ctx = property(_get_ctx)

    def _load_context(self, ctx):
        ctx.db_count = 0
        ctx.transactions = []  # stack of transactions

        if self.has_pooling:
            ctx.db = self._connect_with_pooling(**self.config)
        else:
            ctx.db = self._connect(**self.config)
        self._cursor = ctx.db.cursor()
        ctx.execute = self.execute

        if not hasattr(ctx.db, 'commit'):
            ctx.db.commit = lambda: None

        if not hasattr(ctx.db, 'rollback'):
            ctx.db.rollback = lambda: None

        def commit(unload=True):
            # do db commit and release the connection if pooling is enabled.
            ctx.db.commit()
            if unload and self.has_pooling:
                _unload_context(ctx)

        def rollback():
            # do db rollback and release the connection if pooling is enabled.
            ctx.db.rollback()
            if self.has_pooling:
                _unload_context(ctx)

        ctx.commit = commit
        ctx.rollback = rollback

    def _connect(self, **keywords):
        raise NotImplementedError()

    def _connect_with_pooling(self, **keywords):
        raise NotImplementedError()

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.ctx.db.cursor()
        return self._cursor

    def execute(self, sql: str, params=None) -> int:
        if params is None:
            params = []
        sql = _sql_params(sql, *params)
        try:
            return self.cursor.execute(sql)
        except Exception:
            self.logger.error(f'ERR: {sql}')
            raise

    def query(self, sql: str, params=None, _test=False) -> list:
        if params is None:
            params = []
        if _test:
            return _sql_params(sql, *params)
        self.execute(sql, params)

        rows = [x for x in self.cursor.fetchall()]
        if self.cursor.description:
            json_row = []
            if not rows:
                return json_row
            cols = [x[0] for x in self.cursor.description]
            for row in rows:
                obj = {}
                for prop, val in zip(cols, row):
                    obj[prop] = val
                json_row.append(obj)
            return json_row
        else:
            return rows

    def select(self, table: str, params=None, where: str = '', last: str = ''):
        if params is None:
            params = ['*']
        return self.query(
            f"select {','.join(params if params else [])} from {table} where {where if where else '1=1'} {last}"
        )

    def insert(self, table: str, _last: str = '', _seq: str = None, _test=False, **values):
        def column_format(v):
            return f'`{v}`'

        columns = [x for x in values.keys()]
        if len(columns) > 0:
            sql = f"insert into {table} ({','.join([column_format(x) for x in columns])}) " \
                  f"values ({','.join(['%s'] * len(columns))}) {_last}"
            if _seq is not None:
                sql = self._process_insert_query(sql, table, _seq)

            if _test:
                return sql
            with self.transaction():
                if isinstance(sql, tuple):
                    q1, q2 = sql
                    self.execute(q1, [values[x] for x in columns])
                    self.execute(q2)
                else:
                    self.execute(sql, [values[x] for x in columns])
            try:
                out = self.cursor.fetchone()[0]
            except TypeError:
                out = None
            return out
        else:
            return -1

    def update(self, table: str, where: str = '', _test=False, **values):
        columns = [f'{k} = %s' for k in values.keys()]
        if len(columns) > 0:
            sql = f"update {table} set {','.join(columns)} where {where if where else '1=1'}"
            if _test:
                return sql
            with self.transaction():
                return self.execute(sql, [values[k] for k in values.keys()])
        else:
            return 0

    def delete(self, table: str, where: str = '', _test=False):
        sql = f"delete from {table} where {where if where else '1=1'}"
        if _test:
            return sql
        with self.transaction():
            return self.execute(sql)

    def insert_many(self, table: str, _last: str = '', _test=False, rows: list = None):

        def column_format(v):
            return f'`{v}`'

        def value_format(*args):
            return f"({_sql_params(','.join(['%s'] * len(args)), *args)})"

        if rows and len(rows) > 0:
            with self.transaction():
                if len(rows) > 0:
                    try:
                        columns = [x for x in rows[0].keys()]
                        sql = f"insert into {table} ({','.join([column_format(x) for x in columns])}) values " \
                              f"{','.join([value_format(*[r[x] for x in columns]) for r in rows])} {_last}"
                        if _test:
                            return sql
                        return self.execute(sql)
                    except (TypeError, KeyError):
                        raise ValueError('object structure format error')
                else:
                    if self.insert(table, _last, _test=_test, **rows[0]) != -1:
                        return 1
                    else:
                        return 0
        else:
            return 0

    def transaction(self):
        """Start a transaction."""
        return Transaction(self.ctx)

    def _process_insert_query(self, sql, seq_name, table_name):
        return sql + ";SELECT MAX(%s) FROM %s" % (seq_name, table_name)


class Transaction:
    """Database transaction."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.transaction_count = transaction_count = len(ctx.transactions)

        class base_engine:
            """Transaction Engine used in top level transactions."""

            def __init__(self):
                self.ctx = ctx

            def transact(self):
                self.ctx.commit(unload=False)

            def commit(self):
                self.ctx.commit()

            def rollback(self):
                self.ctx.rollback()

        class sub_engine:
            """Transaction Engine used in sub transactions."""

            def __init__(self):
                self.ctx = ctx

            def query(self, q):
                self.ctx.execute(_sql_params(q, transaction_count))

            def transact(self):
                self.query('SAVEPOINT NADO_%s')

            def commit(self):
                self.query('RELEASE SAVEPOINT NADO_%s')

            def rollback(self):
                self.query('ROLLBACK TO SAVEPOINT NADO_%s')

        class dummy_engine:
            """Transaction Engine used instead of subtransaction_engine
            when sub transactions are not supported."""
            transact = commit = rollback = lambda engine: None

        if self.transaction_count:
            # nested transactions are not supported in some databases
            if self.ctx.config['ignore_nested_transactions']:
                self.engine = dummy_engine()
            else:
                self.engine = sub_engine()
        else:
            self.engine = base_engine()

        self.engine.transact()
        self.ctx.transactions.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def commit(self):
        if len(self.ctx.transactions) > self.transaction_count:
            self.engine.commit()
            self.ctx.transactions = self.ctx.transactions[:self.transaction_count]

    def rollback(self):
        if len(self.ctx.transactions) > self.transaction_count:
            self.engine.rollback()
            self.ctx.transactions = self.ctx.transactions[:self.transaction_count]


try:
    import pymysql

    class MySQL(Driver):

        def _connect(self, **keywords):
            return pymysql.connect(
                host=keywords['host'],
                port=keywords['port'],
                user=keywords['user'],
                passwd=keywords['password'],
                db=keywords['database'],
                charset=keywords['charset']
            )

        def _connect_with_pooling(self, **keywords):
            if getattr(self, '_pool', None) is None:
                self._pool = get_pooled_db(
                    pymysql,
                    host=keywords['host'],
                    port=keywords['port'],
                    user=keywords['user'],
                    passwd=keywords['password'],
                    db=keywords['database'],
                    charset=keywords['charset']
                )
            return self._pool.connection()

        def _process_insert_query(self, query, seq_name, table_name):
            return query, 'SELECT last_insert_id();'

except ImportError:
    pymysql = MySQL = None

try:
    import psycopg2

    class PostgreSQL(Driver):

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._sequences = None

        def _connect(self, **keywords):
            connection = psycopg2.connect(
                host=keywords['host'],
                port=keywords['port'],
                user=keywords['user'],
                password=keywords['password'],
                database=keywords['database']
            )
            connection.set_client_encoding(keywords['charset'])
            return connection

        def _connect_with_pooling(self, **keywords):
            if getattr(self, '_pool', None) is None:
                self._pool = get_pooled_db(
                    psycopg2,
                    host=keywords['host'],
                    port=keywords['port'],
                    user=keywords['user'],
                    password=keywords['password'],
                    database=keywords['database']
                )
            return self._pool.connection()

        def _process_insert_query(self, sql, seq_name, table_name):
            if seq_name is None:
                seq_name = seq_name + "_id_seq"
                if seq_name not in self._get_all_sequences():
                    seq_name = None

            if seq_name:
                sql += "; SELECT currval('%s')" % seq_name

            return sql

        def _get_all_sequences(self):
            if self._sequences is None:
                q = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S'"
                self._sequences = set([c['relname'] for c in self.query(q)])
            return self._sequences

except ImportError:
    psycopg2 = PostgreSQL = None


try:
    import pymssql

    class SqlServer(Driver):
        def _connect(self, **keywords):
            return pymssql.connect(
                login_timeout=keywords['timeout'] if 'timeout' in keywords else None,
                **keywords
            )

        def _connect_with_pooling(self, **keywords):
            if getattr(self, '_pool', None) is None:
                self._pool = get_pooled_db(
                    pymssql,
                    host=keywords['host'],
                    port=keywords['port'],
                    user=keywords['user'],
                    password=keywords['password'],
                    database=keywords['database'],
                    login_timeout=keywords['timeout'] if 'timeout' in keywords else None,
                    charset=keywords['charset']
                )
            return self._pool.connection()

except ImportError:
    pymssql = SqlServer = None
