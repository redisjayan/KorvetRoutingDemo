"""One-shot demo: topic setup, all consumers in threads, then the frontend
producer. Ends with a summary proving priority orders were handled only by
the priority processing application."""
import threading
import time

import config
import priority_consumer
import producer
import setup_topic
import warehouse_consumer


def main():
    setup_topic.ensure_topic()

    stop = threading.Event()
    lock = threading.Lock()
    handled = []  # (consumer_label, order)

    def tracker(label):
        def on_order(order):
            with lock:
                handled.append((label, order))
        return on_order

    threads = [
        threading.Thread(target=warehouse_consumer.run,
                         args=(wh, stop, tracker(wh)), daemon=True)
        for wh in config.WAREHOUSES
    ]
    threads.append(
        threading.Thread(target=priority_consumer.run,
                         args=(stop, tracker("PRIORITY")), daemon=True)
    )
    for t in threads:
        t.start()
    time.sleep(config.CONSUMER_START_GRACE_SEC)

    sent = producer.main()
    sent_ids = {o["order_id"] for o in sent}

    # Wait until every order from this run has been consumed (or timeout).
    deadline = time.time() + config.DEMO_TIMEOUT_SEC
    while time.time() < deadline:
        with lock:
            done = sum(1 for _, o in handled if o["order_id"] in sent_ids)
        if done >= len(sent):
            break
        time.sleep(0.5)

    stop.set()
    for t in threads:
        t.join(timeout=5)

    with lock:
        this_run = [(label, o) for label, o in handled if o["order_id"] in sent_ids]
    report(sent, this_run)


def report(sent, handled):
    total_priority = sum(1 for o in sent if o["priority"])
    print("\n" + "=" * 50)
    print(f"produced  : {len(sent)} orders ({total_priority} priority)")
    ok = True
    for label in config.WAREHOUSES + ["PRIORITY"]:
        mine = [o for l, o in handled if l == label]
        prio = sum(1 for o in mine if o["priority"])
        print(f"{label:<10}: consumed {len(mine)} orders ({prio} priority)")
        if label == "PRIORITY":
            ok = ok and prio == len(mine) == total_priority
        else:
            ok = ok and prio == 0
    ok = ok and len(handled) == len(sent)
    print("routing check:",
          "PASS - priority orders were handled only by the priority consumer"
          if ok else "FAIL - see counts above")
    print("=" * 50)


if __name__ == "__main__":
    main()
