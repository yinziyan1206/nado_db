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
                value = '%{}%'.format(value.replace("%", r"\%").replace("_", r"\_"))
            elif op == 'like_left':
                value = '%{}'.format(value.replace("%", r"\%").replace("_", r"\_"))
                op = 'like'
            elif op == 'like_right':
                value = '{}%'.format(value.replace("%", r"\%").replace("_", r"\_"))
                op = 'like'
        return value, op

    def _base_op(self, column_name, value, op, alias="") -> None:
        value, op = self.like_filter(value, op)
        column_name = f"{alias}.{column_name}" if alias else column_name
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

    def eq(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, '=', alias=alias)

    def ne(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, '<>', alias=alias)

    def lt(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, '<', alias=alias)

    def le(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, '<=', alias=alias)

    def gt(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, '>', alias=alias)

    def ge(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, '>=', alias=alias)

    def like(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, 'like', alias=alias)

    def like_left(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, 'like_left', alias=alias)

    def like_right(self, column_name, value, alias="") -> None:
        self._base_op(column_name, value, 'like_right', alias=alias)

    def include(self, column_name, *values, alias="") -> None:
        format_value = []
        value_wrapper = "'{0}'"
        if not values:
            raise IndexError

        for v in values:
            if v is None:
                format_value.append('NULL')
            elif type(v) in (int, float, decimal.Decimal):
                format_value.append(str(v))
            elif issubclass(v.__class__, Enum):
                format_value.append(value_wrapper.format(str(v.value)))
            elif isinstance(v, datetime.datetime):
                format_value.append(value_wrapper.format(v.strftime('%Y-%m-%d %H:%M:%S')))
            else:
                format_value.append(value_wrapper.format(str(v).replace("'", "''")))

        column_name = f"{alias}.{column_name}" if alias else column_name
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
