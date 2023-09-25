from aiopooldatabases.base.distribute import DistributeDB

mysql_client = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123',
    'db': 'aaa',
}

mongo_client = {
    'port': 3306
}

redis_client = {
    'port': 3306
}

cli = DistributeDB(mysql_client=mysql_client, mongo_client=mongo_client, redis_client=redis_client)
sql = "INSERT INTO a (`name`) VALUES ('焦长豪')"
print(cli.mysql_cli.execute(sql=sql))


# from sqltool.mysql_client import MySqlClient
#
# cli = MySqlClient(
#     host='localhost',
#     port=3306,
#     user='root',
#     password='123',
#     db='aaa'
# )
# print(cli.execute(sql))
