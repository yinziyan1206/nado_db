__author__ = 'ziyan.yin'

import enum
from dataclasses import dataclass, fields, field
import datetime


def data_field(*, default=None, required=False, description='', length=64, exists=True):
    return field(
        default=default,
        init=True,
        metadata={
            'required': required, 'description': description, 'length': length, 'exists': exists
        }
    )


@dataclass
class BaseModel:
    id: int = data_field(default=None, required=False, description='id', length=20)
    version: int = data_field(default=0, required=True, description='乐观锁', length=11)
    deleted: int = data_field(default=0, required=True, description='删除', length=4)
    create_time: datetime.datetime = data_field(default=None, required=False, description='创建日期', length=20)
    modify_time: datetime.datetime = data_field(default=None, required=False, description='修改日期', length=20)

    def __post_init__(self):
        annotations = getattr(self.__class__, '__annotations__')
        for k, v in annotations.items():
            if type(v) is enum.EnumMeta and type((data := getattr(self, k))) is int:
                setattr(self, k, v(data))

    def next_val(self):
        from sequence import flake
        if not self.id:
            while (index := flake()) == 0:
                continue
            self.id = index

    def check_data(self):
        for f in fields(self):
            data = getattr(self, f.name, None)
            if data and f.type in (str, bool, int, float) and len(str(data)) > f.metadata['length']:
                raise ValueError('[%s]长度越界' % f.name)

    def to_table(self) -> dict:
        data = {}
        for f in fields(self):
            if not f.metadata['exists']:
                continue
            data[f.name] = getattr(self, f.name)
            if f.metadata['required'] and data[f.name] is None:
                raise ValueError('[%s]数值不能为null' % f.name)
        return data
