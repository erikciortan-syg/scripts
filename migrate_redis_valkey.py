import redis
import concurrent.futures
import os
import hashlib
import re
from datetime import datetime



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
    'username': os.getenv('VALKEY_USERNAME'),
    'password': os.getenv('VALKEY_PASSWORD'),
    'ssl': True,
    'db': 0,
}

SHARD_TOTAL = int(os.getenv("SHARD_TOTAL", "1"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))

THREADS = 40
BATCH_SIZE = 5000
SCAN_COUNT = 10000

def connect(cfg):
    return redis.Redis(
        host=cfg['host'],
        port=cfg['port'],
        username=cfg['username'],
        password=cfg['password'],
        ssl=cfg['ssl'],
        db=cfg['db'],
        decode_responses=False,
        retry_on_timeout=True,
        socket_timeout=10,
    )

def is_my_key(key: bytes) -> bool:
    key_hash = int(hashlib.md5(key).hexdigest(), 16)
    return key_hash % SHARD_TOTAL == SHARD_INDEX

def migrate_batch(keys, db_index):
    if not keys:
        return 0, 0, 0, 0, 0

    src_cfg = SRC_REDIS.copy()
    src_cfg['db'] = db_index
    dst_cfg = DST_REDIS.copy()
    dst_cfg['db'] = db_index

    src = connect(src_cfg)
    dst = connect(dst_cfg)
    pipe = dst.pipeline(transaction=False)

    scanned = 0
    shard_hits = 0
    migrated = 0
    skipped_existing = 0
    skipped_expired = 0
    keys_to_delete = []

    for key in keys:
        scanned += 1
        if not is_my_key(key):
            continue
        shard_hits += 1
        try:
            ttl = src.pttl(key)
            if ttl == -2:
                skipped_expired += 1
                continue
            if dst.exists(key):
                skipped_existing += 1
                keys_to_delete.append(key)
                continue

            key_type = src.type(key)

            if key_type == b'string':
                value = src.get(key)
                if value is not None:
                    pipe.set(key, value, px=ttl if ttl > 0 else None)
                    keys_to_delete.append(key)
            elif key_type == b'hash':
                value = src.hgetall(key)
                if value:
                    pipe.hset(name=key, mapping=value)
                    keys_to_delete.append(key)
            elif key_type == b'list':
                items = src.lrange(key, 0, -1)
                if items:
                    pipe.rpush(key, *items)
                    keys_to_delete.append(key)
            elif key_type == b'set':
                members = src.smembers(key)
                if members:
                    pipe.sadd(key, *members)
                    keys_to_delete.append(key)
            elif key_type == b'zset':
                members = src.zrange(key, 0, -1, withscores=True)
                if members:
                    pipe.zadd(key, dict(members))
                    keys_to_delete.append(key)
            else:
                print(f"Skipping unsupported key type: {key_type.decode()} ({key})", flush=True)
                continue

        except Exception as e:
            print(f"Failed to migrate key: {key} ({e})", flush=True)

    try:
        results = pipe.execute()
        # Only delete keys that were successfully migrated
        successful_keys = [k for k, r in zip(keys_to_delete, results) if r is True or r == b'OK' or isinstance(r, int)]
        if successful_keys:
            try:
                src.delete(*successful_keys)
                print(f"🗑️ Deleted {len(successful_keys)} keys from source. Example keys: {[k.hex() for k in successful_keys[:5]]}", flush=True)
            except Exception as e:
                print(f"⚠️ Failed to delete keys from source: {e}", flush=True)
        migrated += len(successful_keys)
    except redis.exceptions.RedisError as e:
        print(f"❌ Pipeline failed in DB {db_index}: {e}", flush=True)
        # If pipeline fails, do not delete any keys
        
    if shard_hits == 0:
        print(f"SHARD {SHARD_INDEX} scanned {scanned} keys in DB {db_index}, but no keys matched this shard", flush=True)
    
    print(f"🧩 DB {db_index} | SHARD {SHARD_INDEX} scanned {scanned} keys, matched {shard_hits}, migrated {migrated}, existing {skipped_existing}, expired {skipped_expired}", flush=True)
    return scanned, shard_hits, migrated, skipped_existing, skipped_expired

def main():
    per_db_stats = {}
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
                print(f"SHARD {SHARD_INDEX} scanning DB {db_index}... got {len(keys)} keys from SCAN", flush=True)
                for i in range(0, len(keys), BATCH_SIZE):
                    batch = keys[i:i + BATCH_SIZE]
                    futures.append(executor.submit(migrate_batch, batch, db_index))
                if cursor == 0:
                    break

            for future in concurrent.futures.as_completed(futures):
                scanned, shard_hits, migrated, skipped_existing, skipped_expired = future.result()
                total += migrated
                per_db_stats.setdefault(db_index, {"scanned": 0, "matched": 0, "migrated": 0, "skipped_existing": 0, "skipped_expired": 0})
                per_db_stats[db_index]["scanned"] += scanned
                per_db_stats[db_index]["matched"] += shard_hits
                per_db_stats[db_index]["migrated"] += migrated
                per_db_stats[db_index]["skipped_existing"] += skipped_existing
                per_db_stats[db_index]["skipped_expired"] += skipped_expired
                print(f"[{datetime.now().isoformat()}] Progress: {total} keys migrated", flush=True)
            try:
                with open(f"/tmp/progress-shard{SHARD_INDEX}.log", "a") as log_file:
                    log_file.write(f"[{datetime.now().isoformat()}] Progress: {total} keys migrated\n")
            except Exception as e:
                    print(f"⚠️ Failed to write progress log: {e}", flush=True)

    if total == 0:
        with open(f"/tmp/progress-shard{SHARD_INDEX}.log", "a") as log_file:
            log_file.write("No keys migrated in this shard.\n")
    else:
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
        print(f"✅ DB {db_index} finished for SHARD {SHARD_INDEX}", flush=True)
    
    with open(f"/tmp/progress-shard{SHARD_INDEX}.log", "a") as log_file:
        for db_index, stats in per_db_stats.items():
            log_file.write(
                f"DB {db_index} - Scanned: {stats['scanned']}, "
                f"Matched: {stats['matched']}, "
                f"Migrated: {stats['migrated']}, "
                f"Skipped (existing): {stats['skipped_existing']}, "
                f"Skipped (expired): {stats['skipped_expired']}\n"
            )
        log_file.write(f"✅ Total keys migrated: {total}\n")

if __name__ == '__main__':
    main()