"""Shared consumer loop: attach to exactly one partition and process orders.

`assign()` is used instead of `subscribe()` on purpose: with subscribe(), the
group coordinator hands out partitions arbitrarily, so a warehouse consumer
could be given the priority partition. Manual assignment pins each application
to the partition it owns.
"""
import json

from kafka import KafkaConsumer, TopicPartition

import config


def consume_partition(partition, group_id, label, process, stop_event=None,
                      on_order=None, auto_offset_reset="earliest"):
    """Poll one partition forever (or until stop_event is set).

    process(order, is_priority) handles each order; on_order(order) is an
    optional hook used by run_demo.py to collect results.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    consumer.assign([TopicPartition(config.TOPIC, partition)])
    print(f"[{label:<9}] online - reading partition {partition}")
    try:
        while stop_event is None or not stop_event.is_set():
            for messages in consumer.poll(timeout_ms=config.CONSUMER_POLL_TIMEOUT_MS).values():
                for msg in messages:
                    headers = dict(msg.headers or [])
                    is_priority = headers.get(config.PRIORITY_HEADER) == b"1"
                    process(msg.value, is_priority)
                    if on_order:
                        on_order(msg.value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"[{label:<9}] offline")
