# encoding: utf-8
from __future__ import absolute_import, unicode_literals
from typing import Type
import threading
from itertools import chain
import os
from pymysql.cursors import DictCursor
from aiopooldatabases.aiopoolmysql.mysql_pool import MysqlConnectWrapper


class Pool(object):

    def __init__(self, connection_class: Type, max_connections=None, db_type=None, **connection_kwargs):
        """
        Mysql连接池
        :param connection_class: MysqlConnect 类
        :param max_connections: 最大连接数
        :param connection_kwargs: 链接创建参数
        """
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.connection_class = connection_class
        if db_type == 'mysql':
            self.connection_kwargs = connection_kwargs
            self.max_connections = max_connections
            self.connection_kwargs.setdefault('cursorclass', DictCursor)
            self.autocommit = self.connection_kwargs.setdefault('autocommit', True)
            if issubclass(self.connection_class, MysqlConnectWrapper):
                self.connection_kwargs['pool'] = self

        self.pid = None
        self._created_connections = None
        self._available_connections = None
        self._in_use_connections = None
        self._check_lock = None
        self.reset()

    def reset(self):
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self._check_lock = threading.Lock()

    def _checkpid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                self.disconnect()
                self.reset()

    def get_connection(self):
        self._checkpid()
        try:
            connection = self._available_connections.pop()
            connection.ping()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    # 创建新连接
    def make_connection(self):
        """
        Create a new connection
        """
        if self._created_connections >= self.max_connections:
            raise IOError("Too many connections")
        self._created_connections += 1

        return self.connection_class(**self.connection_kwargs)

    # 将连接释放回池
    def release(self, connection):
        """
        Releases the connection back to the pool
        """
        self._checkpid()
        if connection.pid != self.pid:
            return
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

    # 断开池中的所有连接
    def disconnect(self):
        """
        Disconnects all connections in the pool
        """
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()
