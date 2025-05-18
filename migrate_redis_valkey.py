import redis
import concurrent.futures
import os

SRC_REDIS = {
    'host': 'master.production-us-east-1-redis.wpx1el.use1.cache.amazonaws.com',
    'port': 6379,
    'username': os.getenv('REDIS_USERNAME'),
    'password': os.getenv('REDIS_PASSWORD'),
    'ssl': True,
    'db': 0,
}

DST_REDIS = {
    'host': 'master.production-us-east-1-valkey.wpx1el.use1.cache.amazonaws.com',
    'port': 6379,
    'username': os.getenv('VALKEY_USERNAME', 'default'),
    'password': os.getenv('VALKEY_PASSWORD'),
    'ssl': True,
    'db': 0,
}

THREADS = 10
BATCH_SIZE = 500
SCAN_COUNT = 1000

def connect(cfg):
    return redis.Redis(
        host=cfg['host'],
        port=cfg['port'],
        username=cfg['username'],
        password=cfg['password'],
        ssl=cfg['ssl'],
        db=cfg['db'],
        decode_responses=False,
    )

def migrate_batch(keys):
    if not keys:
        return 0

    src = connect(SRC_REDIS)
    dst = connect(DST_REDIS)
    pipe = dst.pipeline(transaction=False)

    migrated = 0
    for key in keys:
        try:
            ttl = src.pttl(key)
            if ttl == -2:
                continue  # key doesn't exist or is expired
            if dst.exists(key):
                continue  # skip already existing key

            key_type = src.type(key)

            if key_type == b'string':
                value = src.get(key)
                pipe.set(key, value, px=ttl if ttl > 0 else None)
            elif key_type == b'hash':
                value = src.hgetall(key)
                if value:
                    pipe.hmset(key, value)
            elif key_type == b'list':
                items = src.lrange(key, 0, -1)
                if items:
                    pipe.rpush(key, *items)
            elif key_type == b'set':
                members = src.smembers(key)
                if members:
                    pipe.sadd(key, *members)
            elif key_type == b'zset':
                members = src.zrange(key, 0, -1, withscores=True)
                if members:
                    pipe.zadd(key, dict(members))
            else:
                print(f"Skipping unsupported key type: {key_type.decode()} ({key})")
                continue

            migrated += 1
        except Exception as e:
            print(f"Failed to migrate key: {key} ({e})")

    pipe.execute()
    return migrated

def main():
    src = connect(SRC_REDIS)
    cursor = 0
    total = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        while True:
            cursor, keys = src.scan(cursor=cursor, count=SCAN_COUNT)
            for i in range(0, len(keys), BATCH_SIZE):
                batch = keys[i:i + BATCH_SIZE]
                futures.append(executor.submit(migrate_batch, batch))

            if cursor == 0:
                break

        for future in concurrent.futures.as_completed(futures):
            total += future.result()
            print(f"Progress: {total} keys migrated", flush=True)

    print(f"✅ Migration complete. Total keys migrated: {total}")

if __name__ == '__main__':
    main()