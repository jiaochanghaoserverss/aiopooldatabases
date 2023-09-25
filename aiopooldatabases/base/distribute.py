from aiopooldatabases.aiopoolmysql.mysql_client import MysqlClient
from aiopooldatabases.common import const


class DistributeDB:
    def __init__(self, **distribute):
        self.mysql_cli, self.mongodb_cli, self.redis_cli = self.__distribute(distribute)

    @classmethod
    def __distribute(cls, distribute):
        mysql_cli, mongodb_cli, redis_cli = None, None, None,
        db_keys_list = distribute.keys()
        for key in db_keys_list:
            if key.startswith(const.Database.MYSQL):
                host, port, user, password, charset, db = cls.__dict_convert_format__(distribute.get(key), key)
                mysql_cli = MysqlClient(host=host, port=port, user=user, password=password, charset=charset, db=db)
            elif key.startswith(const.Database.MONGODB):
                pass
            else:
                pass
        return mysql_cli, mongodb_cli, redis_cli

    @classmethod
    def __dict_convert_format__(cls, client_dict, cli_type):
        if cli_type.startswith(const.Database.MYSQL):
            host = client_dict.get('host')
            port = client_dict.get('port')
            user = client_dict.get('user')
            password = client_dict.get('password') or client_dict.get('pwd')
            db = client_dict.get('database') or client_dict.get('db')
            charset = client_dict.get('charset') or 'utf8'
            return host, port, user, password, charset, db



