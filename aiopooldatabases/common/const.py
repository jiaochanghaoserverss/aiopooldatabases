class Database:
    MYSQL = 'mysql'
    REDIS = 'redis'
    MONGODB = 'mongodb'
    CHOICES = (
        (MYSQL, 'mysql'),
        (REDIS, 'redis'),
        (MONGODB, 'mongodb'),
    )
