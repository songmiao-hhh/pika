import redis

# db数量
db_num = 4

for i in range(db_num):
    # 连接第一个 Redis
    r1 = redis.Redis(host='localhost', port=9221, db=i)

    # 连接第二个 Redis
    r2 = redis.Redis(host='localhost', port=7000, password='abc', db=i)

    cursor = 0
    # 从第一个 Redis 读数据检测
    while True:
        cursor, keys = r1.scan(cursor)
        for key in keys:
            key_type = r1.type(key)

            if key_type == b'string':
                value1 = r1.get(key)
                value2 = r2.get(key)
            elif key_type == b'list':
                value1 = r1.lrange(key, 0, -1)
                value2 = r2.lrange(key, 0, -1)
            elif key_type == b'set':
                value1 = r1.smembers(key)
                value2 = r2.smembers(key)
            elif key_type == b'zset':
                value1 = r1.zrange(key, 0, -1, withscores=True)
                value2 = r2.zrange(key, 0, -1, withscores=True)
            elif key_type == b'hash':
                value1 = r1.hgetall(key)
                value2 = r2.hgetall(key)
            else:
                print(f"Unknow type")
        
            # 检查两个Redis实例中的值是否相同
            if value1 != value2:
                print(f"Key '{key.decode()}' is NOT consistent'")

        if cursor == 0:
            break
    
    # 从第二个 Redis 读数据检测
    while True:
        cursor, keys = r2.scan(cursor)
        for key in keys:
            key_type = r2.type(key)

            if key_type == b'string':
                value1 = r1.get(key)
                value2 = r2.get(key)
            elif key_type == b'list':
                value1 = r1.lrange(key, 0, -1)
                value2 = r2.lrange(key, 0, -1)
            elif key_type == b'set':
                value1 = r1.smembers(key)
                value2 = r2.smembers(key)
            elif key_type == b'zset':
                value1 = r1.zrange(key, 0, -1, withscores=True)
                value2 = r2.zrange(key, 0, -1, withscores=True)
            elif key_type == b'hash':
                value1 = r1.hgetall(key)
                value2 = r2.hgetall(key)
            else:
                print(f"Unknow type")
        
            # 检查两个Redis实例中的值是否相同
            if value1 != value2:
                print(f"Key '{key.decode()}' is NOT consistent'")

        if cursor == 0:
            break
