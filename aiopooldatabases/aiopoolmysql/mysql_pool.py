# encoding: utf-8
from __future__ import absolute_import, unicode_literals
import os
import pymysql
from aiomysql.connection import Connection


class MysqlConnectWrapper(pymysql.connections.Connection):
    def __init__(self, *args, **kwargs):
        self.pool = kwargs.pop('pool', None)
        assert self.pool is not None
        self.pid = os.getpid()
        super(MysqlConnectWrapper, self).__init__(*args, **kwargs)

    def close(self):
        super(MysqlConnectWrapper, self).close()


    # 释放
    def release(self):
        self.pool.release(self)

    def cursor(self, cursor_class=None):
        cursor = super(MysqlConnectWrapper, self).cursor(cursor_class)
        close = cursor.close

        def __close(*args, **kwargs):
            close()
            self.release()

        cursor.close = __close

        return cursor
