__author__ = 'ziyan.yin'
__describe__ = 'snowflake algorithm'

import datetime
import time
import os
import socket

_last_point = 0
_last_sequence = 0
_sequence_length = 10
_host_id = socket.gethostbyname(socket.getfqdn(socket.gethostname()))
_machine_id = (int(_host_id.split('.')[-1]) << 4) + (os.getpid() % 16)
_start_point = datetime.datetime(2020, 1, 1).timestamp()


def flake():
    global _last_sequence
    global _last_point

    point = int((time.time() - _start_point) * 100)
    if _last_point == point:
        count = _last_sequence + 1
        if count > (1 << _sequence_length):
            return 0
    else:
        count = 0
        _last_point = point
    _last_sequence = count
    return (_last_point << _sequence_length + 12) + (_machine_id << _sequence_length) + count
