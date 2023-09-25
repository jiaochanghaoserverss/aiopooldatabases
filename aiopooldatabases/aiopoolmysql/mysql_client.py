from aiopooldatabases.base.pool import Pool
import time
import logging
from aiopooldatabases.aiopoolmysql.mysql_pool import MysqlConnectWrapper

from aiopooldatabases.common.log import logger


class MysqlClient:
    def __init__(self, **config):
        self.pool = Pool(connection_class=MysqlConnectWrapper, db_type='mysql', **config)

    def execute(self, sql, args=None, fail_raise=False, cursor_class=None):
        return self._execute(
            sql=sql,
            args=args,
            callback_func=lambda c: c.result,
            log_flag='execute',
            fail_raise=fail_raise,
            cursor_class=cursor_class
        )

    def _execute(
        self,
        *,
        sql,
        args,
        callback_func,
        log_flag,
        default_ret=None,
        fail_raise=False,
        cursor_class=None,
        many=False
    ):
        ret = default_ret
        try:
            start = time.time()
            with self.pool.get_connection().cursor(cursor_class) as cursor:
                if many:
                    cursor.result = cursor.executemany(sql, args)
                else:
                    cursor.result = cursor.execute(sql, args)
                ret = callback_func(cursor)
                if not self.pool.autocommit:
                    cursor.execute("commit")
            logger.info("sql %s finish %fs: %s %r", log_flag, time.time() - start, sql, args)
        except Exception as e:
            logger.error("sql %s error: %s %r", log_flag, sql, args, exc_info=True)
            if fail_raise:
                raise e
        return ret
