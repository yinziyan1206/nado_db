#!/usr/bin/python3
__author__ = 'ziyan.yin'

import datetime
import decimal
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
    def like_filter(value, op):
        if op.startswith('like'):
            value = str(value).replace("'", "''")
            if op == 'like':
                value = f'%{value.replace("%", "[%]").replace("_", "[_]")}%'
            elif op == 'like_left':
                value = f'%{value.replace("%", "[%]").replace("_", "[_]")}'
                op = 'like'
            elif op == 'like_right':
                value = f'{value.replace("%", "[%]").replace("_", "[_]")}%'
                op = 'like'
        return value, op

    def _base_op(self, column_name, value, op) -> None:
        value, op = self.like_filter(value, op)
        if value is None:
            self._condition.append(f"{column_name} is NULL")
        elif type(value) in (int, float, decimal.Decimal):
            self._condition.append(f"{column_name} {op} {value}")
        elif issubclass(value.__class__, Enum):
            self._condition.append(f"{column_name} {op} {value._value_}")
        elif isinstance(value, datetime.datetime):
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
            value = str(value).replace("'", "''")
            self._condition.append(f"{column_name} {op} '{value}'")

    def eq(self, column_name, value) -> None:
        self._base_op(column_name, value, '=')

    def ne(self, column_name, value) -> None:
        self._base_op(column_name, value, '<>')

    def lt(self, column_name, value) -> None:
        self._base_op(column_name, value, '<')

    def le(self, column_name, value) -> None:
        self._base_op(column_name, value, '<=')

    def gt(self, column_name, value) -> None:
        self._base_op(column_name, value, '>')

    def ge(self, column_name, value) -> None:
        self._base_op(column_name, value, '>=')

    def like(self, column_name, value) -> None:
        self._base_op(column_name, value, 'like')

    def like_left(self, column_name, value) -> None:
        self._base_op(column_name, value, 'like_left')

    def like_right(self, column_name, value) -> None:
        self._base_op(column_name, value, 'like_right')

    def include(self, column_name, *values) -> None:
        format_value = []
        for v in values:
            if v is None:
                format_value.append('NULL')
            elif type(v) in (int, float, decimal.Decimal):
                format_value.append(v)
            elif issubclass(v.__class__, Enum):
                format_value.append(v._value_)
            elif isinstance(v, datetime.datetime):
                format_value.append(v.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                format_value.append(str(v).replace("'", "''"))

        self._condition.append(f"{column_name} in ({','.join(format_value)})")

    def last(self, sql) -> None:
        self._last = sql

    def add_order(self, order, asc=True):
        self._order.append(f'{order} {"asc" if asc else "desc"}')

    @property
    def order(self) -> str:
        if len(self._order) > 0:
            return f" ORDER BY {','.join(self._order)} "
        return ''

    @property
    def sql_segment(self):
        return f"{' and '.join(self._condition)} {self.order} {self._last}"
