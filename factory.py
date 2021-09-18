#!/usr/bin/python3
__author__ = 'ziyan.yin'

from dataclasses import is_dataclass, field, fields, asdict
from typing import Union

from .aiodriver import AsyncDriver
from .driver import Driver, sql_params


def data_field(*, default=None, required=False, description='', length=64):
    return field(
        default=default,
        init=True,
        metadata={
            'required': required, 'description': description, 'length': length
        }
    )


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

    def __init__(self, driver: Union[Driver, AsyncDriver]):
        self.db = driver

    def save(self, obj, cursor=None, _test=False):
        if self.check(obj):
            table = obj.table
            seq = obj.primary if hasattr(obj, 'primary') else 'id'
            params = asdict(obj)
            if getattr(obj, seq, None):
                updates = [f'{x}=values({x})' for x in params.keys() if x != seq]
                if issubclass(self.db.__class__, AsyncDriver):
                    return self.db.insert(
                        table, cursor=cursor, _seq=seq, _test=_test,
                        _last=f"ON DUPLICATE KEY UPDATE {','.join(updates)}",
                        **params
                    )
                else:
                    return self.db.insert(
                        table, _seq=seq, _test=_test,
                        _last=f"ON DUPLICATE KEY UPDATE {','.join(updates)}",
                        **params
                    )
            else:
                return self.db.insert(table, _seq=seq, _test=_test, **params)

    def save_batch(self, *args, cursor=None, _test=False):
        if len(args) > 1 and self.check(args[0]):
            table = args[0].table
            seq = args[0].primary if hasattr(args[0], 'primary') else 'id'
            params = asdict(args[0])
            updates = [f'{x}=values({x})' for x in params.keys() if x != seq]
            items = []
            for x in args:
                data = asdict(x)
                if seq in data and not data[seq]:
                    del data[seq]
                items.append(data)
            if issubclass(self.db.__class__, AsyncDriver):
                return self.db.insert_many(
                    table, cursor=cursor, _test=_test,
                    _last=f"ON DUPLICATE KEY UPDATE {','.join(updates)}",
                    rows=items
                )
            else:
                return self.db.insert_many(
                    table, _test=_test,
                    _last=f"ON DUPLICATE KEY UPDATE {','.join(updates)}",
                    rows=items
                )
        elif len(args) == 1:
            return self.save(args[0], cursor=cursor, _test=_test)
        else:
            raise ValueError

    def delete(self, obj, cursor=None, _test=False):
        if self.check(obj):
            table = obj.table
            seq = obj.primary if hasattr(obj, 'primary') else 'id'
            if key := getattr(obj, seq, None):
                if issubclass(self.db.__class__, AsyncDriver):
                    return self.db.delete(table, _test=_test, cursor=cursor, where=sql_params(f"{seq} = {{}}", key))
                else:
                    return self.db.delete(table, _test=_test, where=sql_params(f"{seq} = {{}}", key))
            else:
                raise ValueError('data obj has no sequence')
        return 0

    def get_by_id(self, obj, _test=False):
        if self.check(obj):
            table = obj.table
            seq = obj.primary if hasattr(obj, 'primary') else 'id'
            params = [x['name'] for x in properties(obj)]
            if key := getattr(obj, seq, None):
                return self.db.query(
                    f"select {','.join(params)} from {table} where {sql_params(f'{seq} = {{}}', key)}",
                    _test=_test
                )
            else:
                raise ValueError('data obj has no sequence')

    @staticmethod
    def check(obj) -> bool:
        if is_dataclass(obj):
            if hasattr(obj, 'table'):
                return True
            else:
                raise TypeError('data obj is not mapping to data table')
        else:
            raise TypeError('data obj is not dataclass')
