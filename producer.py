"""Frontend application: submits orders and routes them by content.

Routing rule (content-based, decided producer-side - the broker never
inspects payloads):
  - priority order      -> dedicated priority partition
  - normal order        -> the partition owned by its destination warehouse
Every record also carries an `x-priority` header.
"""
import json
import random
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

import config


def make_order(seq: int) -> dict:
    return {
        "order_id": f"ORD-{seq:04d}",
        "warehouse": random.choice(config.WAREHOUSES),
        "item": random.choice(config.ITEMS),
        "qty": random.randint(1, 5),
        "priority": random.random() < config.PRIORITY_RATIO,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def route(order: dict) -> int:
    """Pick the partition from the order's content."""
    if order["priority"]:
        return config.PRIORITY_PARTITION
    return config.WAREHOUSE_PARTITIONS[order["warehouse"]]


def main(num_orders: int = config.NUM_ORDERS) -> list:
    producer = KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    sent = []
    for seq in range(1, num_orders + 1):
        order = make_order(seq)
        partition = route(order)
        producer.send(
            config.TOPIC,
            key=order["warehouse"],  # stickiness: same warehouse -> same partition
            value=order,
            partition=partition,
            headers=[(config.PRIORITY_HEADER, b"1" if order["priority"] else b"0")],
        )
        target = "PRIORITY" if order["priority"] else order["warehouse"]
        print(f"[{'frontend':<9}] submitted {order['order_id']} -> {target:<9} "
              f"(partition {partition}) {order['item']} x{order['qty']}")
        sent.append(order)
        time.sleep(config.PRODUCE_DELAY_SEC)

    producer.flush()
    producer.close()
    print(f"[{'frontend':<9}] done - {len(sent)} orders submitted")
    return sent


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else config.NUM_ORDERS)
