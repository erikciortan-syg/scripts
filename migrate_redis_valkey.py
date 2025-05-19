import redis
import concurrent.futures
import os
import hashlib
import re


SRC_REDIS = {
    'host': os.getenv('REDIS_HOST'),
    'port': 6379,
    'username': os.getenv('REDIS_USERNAME'),
    'password': os.getenv('REDIS_PASSWORD'),
    'ssl': True,
    'db': 0,
}

DST_REDIS = {
    'host': os.getenv('VALKEY_HOST'),
    'port': 6379,
    'username': os.getenv('VALKEY_USERNAME', 'default'),
    'password': os.getenv('VALKEY_PASSWORD'),
    'ssl': True,
    'db': 0,
}

SHARD_TOTAL = int(os.getenv("SHARD_TOTAL", "1"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))

THREADS = 20
BATCH_SIZE = 1000
SCAN_COUNT = 2000

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

def is_my_key(key: bytes) -> bool:
    key_hash = int(hashlib.md5(key).hexdigest(), 16)
    return key_hash % SHARD_TOTAL == SHARD_INDEX

def migrate_batch(keys, db_index):
    if not keys:
        return 0

    src_cfg = SRC_REDIS.copy()
    src_cfg['db'] = db_index
    dst_cfg = DST_REDIS.copy()
    dst_cfg['db'] = db_index

    src = connect(src_cfg)
    dst = connect(dst_cfg)
    pipe = dst.pipeline(transaction=False)

    migrated = 0
    for key in keys:
        if not is_my_key(key):
            continue
        try:
            ttl = src.pttl(key)
            if ttl == -2:
                continue
            if dst.exists(key):
                continue

            key_type = src.type(key)

            if key_type == b'string':
                value = src.get(key)
                value = src.get(key)
                if value is not None:
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
                print(f"Skipping unsupported key type: {key_type.decode()} ({key})", flush=True)
                continue

            migrated += 1
        except Exception as e:
            print(f"Failed to migrate key: {key} ({e})", flush=True)

    pipe.execute()
    return migrated

def main():
    total = 0
    for db_index in range(16):
        print(f"\n📦 Migrating DB {db_index}...", flush=True)
        SRC_REDIS['db'] = db_index
        cursor = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = []
            src = connect(SRC_REDIS)
            while True:
                cursor, keys = src.scan(cursor=cursor, count=SCAN_COUNT)
                for i in range(0, len(keys), BATCH_SIZE):
                    batch = keys[i:i + BATCH_SIZE]
                    futures.append(executor.submit(migrate_batch, batch, db_index))
                if cursor == 0:
                    break

            for future in concurrent.futures.as_completed(futures):
                total += future.result()
                print(f"Progress: {total} keys migrated", flush=True)
                try:
                    with open(f"/tmp/progress-shard{SHARD_INDEX}.log", "w") as log_file:
                        log_file.write(f"Progress: {total} keys migrated\n")
                except Exception as e:
                    print(f"⚠️ Failed to write progress log: {e}", flush=True)

    with open(f"/tmp/progress-shard{SHARD_INDEX}.log", "a") as log_file:
        log_file.write(f"✅ Migration complete. Total keys migrated: {total}\n")
    print(f"✅ Migration complete. Total keys migrated: {total}", flush=True)

    print("\n🔍 Validation Summary:", flush=True)
    for db_index in range(16):
        src_cfg = SRC_REDIS.copy()
        src_cfg['db'] = db_index
        dst_cfg = DST_REDIS.copy()
        dst_cfg['db'] = db_index

        src_count = connect(src_cfg).dbsize()
        dst_count = connect(dst_cfg).dbsize()
        status = "✅ OK" if src_count == dst_count else "⚠️ Mismatch"
        print(f"DB {db_index}: Source = {src_count}, Destination = {dst_count} --> {status}", flush=True)

if __name__ == '__main__':
    main()