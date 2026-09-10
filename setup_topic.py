"""Create the orders topic: one partition per warehouse + one priority partition."""
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import config


def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers=config.BOOTSTRAP_SERVERS)
    try:
        admin.create_topics(
            [NewTopic(config.TOPIC, config.NUM_PARTITIONS, replication_factor=1)]
        )
        print(f"created topic {config.TOPIC!r} with {config.NUM_PARTITIONS} partitions")
    except TopicAlreadyExistsError:
        print(f"topic {config.TOPIC!r} already exists")
    finally:
        admin.close()

    # Guard: routing needs at least warehouses + 1 partitions.
    probe = KafkaConsumer(bootstrap_servers=config.BOOTSTRAP_SERVERS)
    partitions = probe.partitions_for_topic(config.TOPIC) or set()
    probe.close()
    if len(partitions) < config.NUM_PARTITIONS:
        raise SystemExit(
            f"topic {config.TOPIC!r} has {len(partitions)} partitions but "
            f"{config.NUM_PARTITIONS} are needed - delete it or change TOPIC in config.py"
        )


if __name__ == "__main__":
    ensure_topic()
