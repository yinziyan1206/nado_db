__author__ = 'ziyan.yin'

import copy
import datetime
import itertools
from dataclasses import is_dataclass, fields

from .model import BaseModel
from .aiodriver import AsyncDriver
from .driver import sql_params

BatchInsertError = IndexError('批量数组为空')


def table_name(table: str):
    def wrapper(cls):
        if is_dataclass(cls) and not hasattr(cls, 'table'):
            cls.table = table
        return cls
    return wrapper


def primary_key(item: str):
    def wrapper(cls):
        if is_dataclass(cls) and hasattr(cls, item) and not hasattr(cls, 'primary'):
            cls.primary = item
        return cls

    return wrapper


def properties(cls):
    if is_dataclass(cls):
        return [
            {
                'name': x.name,
                'type': x.type,
                'default': x.default,
                'description': x.metadata['description'],
                'required': x.metadata['required'],
                'length': x.metadata['length']
            }
            for x in fields(cls)
        ]
    else:
        return []


class RepositoryFactory:
    __slots__ = ['db', 'auto_increment']

    def __init__(self, auto_increment: bool, driver: AsyncDriver):
        self.auto_increment = auto_increment
        self.db = driver

    async def save(self, obj: BaseModel, cursor=None, _test=False) -> int:
        self.check(obj)
        table = getattr(obj, 'table')
        seq = getattr(obj, 'primary', 'id')
        params = obj.to_table()
        if getattr(obj, seq, None):
            updates = {x: params[x] for x in params.keys() if x != seq}
            updates['version'] += 1
            updates['modify_time'] = datetime.datetime.now()
            return await self.db.update(
                table,
                cursor=cursor,
                _test=_test,
                where=f"{seq} = {params[seq]} and deleted = 0 and version = {params['version']}",
                **updates
            )
        else:
            if not self.auto_increment:
                obj.next_val()
                seq = None
            params['create_time'] = datetime.datetime.now()
            return await self.db.insert(
                table,
                cursor=cursor,
                _seq=seq, _test=_test,
                **params
            )

    async def create_batch(self, *args: BaseModel, cursor=None, _test=False) -> int:
        if len(args) < 1:
            raise BatchInsertError

        self.check(args[0])
        table = getattr(args[0], 'table')
        items = []

        if len(args) > 1:
            for x in args:
                data = x.to_table()
                items.append(data)
            return await self.db.insert_many(
                table, cursor=cursor, _test=_test,
                rows=items
            )
        else:
            return await self.save(args[0], cursor=cursor, _test=_test)

    async def update_batch(self, *args: BaseModel, cursor=None, _test=False) -> int:
        if len(args) < 1:
            raise BatchInsertError

        ids = []

        self.check(args[0])
        table = getattr(args[0], 'table')
        seq = getattr(args[0], 'primary', 'id')
        updates = {
            x.name: [] for x in fields(args[0]) if x.name != 'version' and x.name != seq and x.metadata['exists']
        }
        versions = []
        values = copy.deepcopy(updates)
        now = datetime.datetime.now()

        for item in args:
            self.check(item)
            item.modify_time = now
            params = item.to_table()
            for k in updates:
                updates[k].append(
                    f"WHEN {seq} = {item.id} AND version = {item.version} THEN {'{}'}\n"
                )
                values[k].append(params[k])
            ids.append(str(getattr(item, seq)))
            versions.append(
                f"WHEN {seq} = {item.id} AND version = {item.version} THEN {item.version + 1}\n"
            )

        setter = (f'\n{x} = CASE\n{"".join(updates[x])} ELSE {x} END' for x in updates)
        versions = [f'\nversion = CASE\n{"".join(versions)} ELSE version END']
        sql = f"""UPDATE {table} SET {', '.join(itertools.chain(setter, versions))} WHERE id IN ({','.join(ids)})"""
        params = []
        for _, v in values.items():
            params.extend(v)

        if _test:
            return sql_params(sql, *params)
        return await self.db.execute(sql, params=params, cursor=cursor)

    async def save_batch(self, *args: BaseModel, cursor=None, _test=False) -> int:
        if len(args) < 1:
            raise BatchInsertError

        self.check(args[0])
        table = getattr(args[0], 'table')
        seq = getattr(args[0], 'primary', 'id')
        updates = [
            f'{x.name}=values({x.name})'
            for x in fields(args[0]) if x.name != seq and x.metadata['exists']
        ]
        items = []

        if len(args) > 1:
            for x in args:
                data = x.to_table()
                items.append(data)
            return await self.db.insert_many(
                table, cursor=cursor, _test=_test,
                _last=f"ON DUPLICATE KEY UPDATE {','.join(updates)}",
                rows=items
            )
        else:
            return await self.save(args[0], cursor=cursor, _test=_test)

    async def destroy(self, obj: BaseModel, cursor=None, _test=False):
        self.check(obj)
        table = getattr(obj, 'table')
        seq = getattr(obj, 'primary', 'id')
        if key := getattr(obj, seq, None):
            return await self.db.delete(
                table, _test=_test, cursor=cursor, where=sql_params(f"{seq} = {{}}", key)
            )
        return 0

    async def delete(self, obj: BaseModel, cursor=None, _test=False):
        self.check(obj)
        table = getattr(obj, 'table')
        seq = getattr(obj, 'primary', 'id')
        if key := getattr(obj, seq, None):
            return await self.db.update(
                table, where=sql_params(f"{seq} = {{}}", key), _test=_test, cursor=cursor, deleted=1
            )
        return 0

    def get(self, obj: BaseModel, _test=False):
        self.check(obj)
        table = getattr(obj, 'table')
        seq = getattr(obj, 'primary', 'id')
        params = [x['name'] for x in properties(obj)]
        if key := getattr(obj, seq, None):
            return self.db.query(
                f"select {','.join(params)} from {table} where {sql_params(f'{seq} = {{}}', key)}",
                _test=_test
            )
        else:
            raise ValueError('data obj has no sequence')

    @staticmethod
    def check(obj):
        if issubclass(type(obj), BaseModel):
            if not getattr(obj, 'table', None):
                raise TypeError('data obj is not mapping to data table')
            obj.check_data()
        else:
            raise TypeError('data obj is not dataclass')
