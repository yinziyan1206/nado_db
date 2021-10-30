#!/usr/bin/python3
__author__ = 'ziyan.yin'

import datetime
import decimal
from copy import copy
from enum import Enum


class QueryWrapper:
    """
        SQL Wrapper to select obj or dict
    """

    __slots__ = ['_condition', '_order', '_last']

    def __init__(self):
        self._condition: list = ['1=1']
        self._order: list = []
        self._last: str = ""

    @staticmethod
    def _like_filter(value, op):
        if op.startswith('like'):
            value = str(value).replace("'", "''")
            if op == 'like' or op == 'not like':
                value = '%{}%'.format(value.replace("%", r"\%").replace("_", r"\_"))
            elif op == 'like_left':
                value = '%{}'.format(value.replace("%", r"\%").replace("_", r"\_"))
                op = 'like'
            elif op == 'like_right':
                value = '{}%'.format(value.replace("%", r"\%").replace("_", r"\_"))
                op = 'like'
        return value, op

    @staticmethod
    def _format_value(v) -> str:
        value_wrapper = "'{0}'"

        if v is None:
            return 'NULL'
        elif type(v) in (int, float, decimal.Decimal):
            return str(v)
        elif issubclass(v.__class__, Enum):
            return value_wrapper.format(str(v.value))
        elif isinstance(v, datetime.datetime):
            return value_wrapper.format(v.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            return value_wrapper.format(str(v).replace("'", "''").replace('\\', '\\\\'))

    def _base_op(self, column_name, value, op, alias="") -> "QueryWrapper":
        value, op = self._like_filter(value, op)
        column_name = f"{alias}.{column_name}" if alias else column_name

        if isinstance(value, datetime.datetime):
            value = value.strftime('%Y-%m-%d %H:%M:%S')
            if op in ('>', '>='):
                self._condition.append(f"{column_name} {op} '{value}.000000'")
            elif op in ('<', '<='):
                self._condition.append(f"{column_name} {op} '{value}.999999'")
            else:
                self._condition.append(f"{column_name} {op} '{value}'")
        elif isinstance(value, datetime.date):
            value = str(value)
            if op in ('>', '>='):
                self._condition.append(f"{column_name} {op} '{value} 00:00:00.000000'")
            elif op in ('<', '<='):
                self._condition.append(f"{column_name} {op} '{value} 23:59:59.999999'")
            else:
                self._condition.append(f"{column_name} {op} '{value}'")
        else:
            value = self._format_value(value)
            self._condition.append(f"{column_name} {op} {value}")

        return self

    def add_raw_condition(self, condition) -> "QueryWrapper":
        self._condition.append(f'({condition})')
        return self

    def eq(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, '=', alias=alias)

    def ne(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, '<>', alias=alias)

    def lt(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, '<', alias=alias)

    def le(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, '<=', alias=alias)

    def gt(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, '>', alias=alias)

    def ge(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, '>=', alias=alias)

    def like(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, 'like', alias=alias)

    def not_like(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, 'not like', alias=alias)

    def like_left(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, 'like_left', alias=alias)

    def like_right(self, column_name, value, alias="") -> "QueryWrapper":
        return self._base_op(column_name, value, 'like_right', alias=alias)

    def include(self, column_name, *values, alias="") -> "QueryWrapper":
        format_value = []
        if not values:
            raise IndexError

        for v in values:
            format_value.append(self._format_value(v))

        column_name = f"{alias}.{column_name}" if alias else column_name
        self._condition.append(f"{column_name} in ({','.join(format_value)})")
        return self

    def between(self, column_name, left, right, alias="") -> "QueryWrapper":
        column_name = f"{alias}.{column_name}" if alias else column_name
        self._condition.append(
            f"{column_name} between ({self._format_value(left)} and {self._format_value(right)})"
        )
        return self

    def not_between(self, column_name, left, right, alias="") -> "QueryWrapper":
        column_name = f"{alias}.{column_name}" if alias else column_name
        self._condition.append(
            f"{column_name} not between ({self._format_value(left)} and {self._format_value(right)})"
        )
        return self

    def exists(self, sub_sql) -> "QueryWrapper":
        self._condition.append(f"exists ({sub_sql})")
        return self

    def not_exist(self, sub_sql) -> "QueryWrapper":
        self._condition.append(f"not exists ({sub_sql})")
        return self

    def last(self, sql) -> None:
        self._last = sql

    def xor(self, query_wrapper: "QueryWrapper") -> "QueryWrapper":
        upper_condition = ' and '.join(self._condition)
        lower_condition = ' and '.join(query_wrapper._condition)
        self._condition = [f'(({upper_condition}) or ({lower_condition}))']
        return self

    def add_order(self, order, asc=True):
        self._order.append(f'{order} {"asc" if asc else "desc"}')

    def clear(self):
        self._condition = ['1=1']
        self._order.clear()
        self._last = ""

    @property
    def order(self) -> str:
        if len(self._order) > 0:
            return f" ORDER BY {','.join(self._order)} "
        return ''

    @property
    def sql_segment(self):
        return f"{' and '.join(self._condition)} {self.order} {self._last}"


class Page:
    """
        SQL Result Page
        Needed: Primary Key[id], OFFSET, SIZE, QueryWrapper
    """
    __slots__ = ["offset", "size", "record", "total", "wrapper"]

    def __init__(self, query_wrapper: QueryWrapper, offset: int = 0, size: int = 10):
        self.offset: int = offset
        self.size: int = size
        self.total: int = 0
        self.record: list = []
        self.wrapper: QueryWrapper = copy(query_wrapper)
        self._structure()

    def next(self):
        if self.total >= self.offset + self.size:
            self.offset += self.size
            self.record.clear()
            self._structure()
        return self

    def prev(self):
        self.offset = max(0, self.offset - self.size)
        self.record.clear()
        self._structure()
        return self

    def _structure(self):
        self.wrapper.last(f"LIMIT {self.size} OFFSET {self.offset}")
