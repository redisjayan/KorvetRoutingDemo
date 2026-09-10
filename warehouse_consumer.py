"""Warehouse fulfilment service: processes normal orders for one warehouse.

Usage: python warehouse_consumer.py [WAREHOUSE_ID]   (default: first in config)
"""
import sys
import time

import config
from consumer_base import consume_partition


def run(warehouse, stop_event=None, on_order=None):
    def process(order, is_priority):
        time.sleep(config.PROCESSING_TIME_SEC)  # simulated fulfilment work
        flag = "  <-- UNEXPECTED priority order!" if is_priority else ""
        print(f"[{warehouse:<9}] fulfilled {order['order_id']}: "
              f"{order['item']} x{order['qty']}{flag}")

    consume_partition(
        partition=config.WAREHOUSE_PARTITIONS[warehouse],
        group_id=f"fulfilment-{warehouse}",
        label=warehouse,
        process=process,
        stop_event=stop_event,
        on_order=on_order,
    )


if __name__ == "__main__":
    wh = sys.argv[1] if len(sys.argv) > 1 else config.WAREHOUSES[0]
    if wh not in config.WAREHOUSE_PARTITIONS:
        raise SystemExit(f"unknown warehouse {wh!r}; choose from {config.WAREHOUSES}")
    run(wh)
