"""
Redis CDC Delete Consumer — дополнение к stream-reactor Redis Sink Connector.

Зачем: stream-reactor НЕ умеет физически удалять ключи из Redis при DELETE из PG.
  Он ставит __deleted="true" (soft delete). Задание требует реальный Delete.

Как работает:
  1. Читает JSON-сообщения из Kafka (Debezium source использует JsonConverter)
  2. При op='d' извлекает id из before и делает DEL в Redis
  3. Задержка 3 сек чтобы stream-reactor успел обработать SET первым
"""

import json
import os
import sys
import time
import redis
from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

TOPICS = [
    "university.public.student",
    "university.public.student_group",
]

KEY_PREFIX_MAP = {
    "university.public.student": "student-",
    "university.public.student_group": "student_group-",
}


def main():
    print(f"[redis-cdc-delete] Kafka: {KAFKA_BOOTSTRAP}", flush=True)
    print(f"[redis-cdc-delete] Redis: {REDIS_HOST}:{REDIS_PORT}", flush=True)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print("[redis-cdc-delete] Redis OK", flush=True)

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "redis-cdc-delete-consumer-v2",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    print("[redis-cdc-delete] Consumer запущен, ожидание DELETE-событий...", flush=True)

    deleted_count = 0

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[redis-cdc-delete] Kafka error: {msg.error()}", flush=True)
            continue

        try:
            topic = msg.topic()
            prefix = KEY_PREFIX_MAP.get(topic)
            if not prefix:
                continue

            raw = msg.value()
            if not raw:
                continue

            value = json.loads(raw.decode("utf-8"))

            payload = value.get("payload", value)
            op = payload.get("op")
            if op != "d":
                continue

            before = payload.get("before", {})
            if not before:
                continue

            record_id = before.get("id")
            if not record_id:
                continue

            redis_key = f"{prefix}{record_id}"

            time.sleep(3)

            deleted = r.delete(redis_key)
            if deleted:
                deleted_count += 1
                print(f"[redis-cdc-delete] DEL {redis_key} (total: {deleted_count})", flush=True)
            else:
                print(f"[redis-cdc-delete] Key already gone: {redis_key}", flush=True)

        except Exception as e:
            print(f"[redis-cdc-delete] Error: {e}", flush=True)


if __name__ == "__main__":
    main()
