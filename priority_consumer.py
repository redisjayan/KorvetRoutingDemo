"""Priority processing application: the ONLY consumer of priority orders."""
import time

import config
from consumer_base import consume_partition

LABEL = "PRIORITY"


def run(stop_event=None, on_order=None):
    def process(order, is_priority):
        time.sleep(config.PRIORITY_PROCESSING_TIME_SEC)  # expedited handling
        print(f"[{LABEL:<9}] EXPEDITED {order['order_id']} for {order['warehouse']}: "
              f"{order['item']} x{order['qty']} "
              f"(header {config.PRIORITY_HEADER}={'1' if is_priority else '0'})")

    consume_partition(
        partition=config.PRIORITY_PARTITION,
        group_id="priority-fulfilment",
        label=LABEL,
        process=process,
        stop_event=stop_event,
        on_order=on_order,
    )


if __name__ == "__main__":
    run()
