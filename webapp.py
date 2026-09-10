"""Web UI to watch the routing live.

Run:  python webapp.py         (needs a broker at config.BOOTSTRAP_SERVERS)
Open: http://127.0.0.1:8000

One process hosts everything the demo needs: a producer endpoint the page
calls to submit orders, the four pinned consumers (three warehouses + the
priority application), and a Server-Sent Events stream that pushes every
"produced" and "consumed" event to the browser. No dependencies beyond
kafka-python; the HTTP server is the standard library.
"""
import itertools
import json
import queue
import random
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from kafka import KafkaProducer

import config
import setup_topic
from consumer_base import consume_partition
from producer import route

UI_FILE = Path(__file__).with_name("web_ui.html")

_subscribers = []
_sub_lock = threading.Lock()
_seq = itertools.count(1)
_kafka_producer = None


def broadcast(event):
    data = json.dumps(event)
    with _sub_lock:
        for q in list(_subscribers):
            q.put(data)


def make_web_order(priority):
    """priority: True/False forces the flag, None rolls the configured ratio."""
    if priority is None:
        priority = random.random() < config.PRIORITY_RATIO
    return {
        "order_id": f"WEB-{next(_seq):04d}",
        "warehouse": random.choice(config.WAREHOUSES),
        "item": random.choice(config.ITEMS),
        "qty": random.randint(1, 5),
        "priority": bool(priority),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def send_order(priority):
    order = make_web_order(priority)
    partition = route(order)
    _kafka_producer.send(
        config.TOPIC,
        key=order["warehouse"],
        value=order,
        partition=partition,
        headers=[(config.PRIORITY_HEADER, b"1" if order["priority"] else b"0")],
    )
    _kafka_producer.flush()
    broadcast({
        "type": "produced",
        "order": order,
        "partition": partition,
        "target": "PRIORITY" if order["priority"] else order["warehouse"],
    })
    return order


def start_consumers(stop_event):
    def make_process(label):
        delay = (config.PRIORITY_PROCESSING_TIME_SEC if label == "PRIORITY"
                 else config.PROCESSING_TIME_SEC)

        def process(order, is_priority):
            time.sleep(delay)  # simulated fulfilment work
            broadcast({
                "type": "consumed",
                "consumer": label,
                "order": order,
                "priority": is_priority,
            })
        return process

    specs = [(wh, config.WAREHOUSE_PARTITIONS[wh]) for wh in config.WAREHOUSES]
    specs.append(("PRIORITY", config.PRIORITY_PARTITION))
    for label, partition in specs:
        threading.Thread(
            target=consume_partition,
            kwargs=dict(partition=partition, group_id=f"web-fulfilment-{label}",
                        label=label, process=make_process(label),
                        stop_event=stop_event, auto_offset_reset="latest"),
            daemon=True,
        ).start()


def state_payload():
    return {
        "type": "hello",
        "topic": config.TOPIC,
        "warehouses": config.WAREHOUSES,
        "warehouse_partitions": config.WAREHOUSE_PARTITIONS,
        "priority_partition": config.PRIORITY_PARTITION,
        "burst_size": config.WEB_BURST_SIZE,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/":
            body = UI_FILE.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/state":
            self._json(state_payload())
        elif self.path == "/events":
            self._sse()
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        if self.path != "/api/order":
            self._json({"error": "not found"}, 404)
            return
        if config.WEB_ACCESS_TOKEN and \
                self.headers.get("X-Demo-Token") != config.WEB_ACCESS_TOKEN:
            self._json({"error": "missing or invalid X-Demo-Token"}, 401)
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            body = {}
        count = max(1, min(int(body.get("count", 1)), 50))
        priority = body.get("priority", None)  # True / False / None(=random)
        orders = [send_order(priority) for _ in range(count)]
        self._json({"sent": len(orders), "orders": [o["order_id"] for o in orders]})

    def _sse(self):
        q = queue.Queue()
        with _sub_lock:
            _subscribers.append(q)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(f"data: {json.dumps(state_payload())}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    data = q.get(timeout=15)
                    self.wfile.write(f"data: {data}\n\n".encode())
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with _sub_lock:
                if q in _subscribers:
                    _subscribers.remove(q)


def main():
    global _kafka_producer
    setup_topic.ensure_topic()
    _kafka_producer = KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    stop = threading.Event()
    start_consumers(stop)

    server = ThreadingHTTPServer((config.WEB_HOST, config.WEB_PORT), Handler)
    server.daemon_threads = True
    print(f"routing demo UI -> http://{config.WEB_HOST}:{config.WEB_PORT}")
    if config.WEB_HOST != "127.0.0.1" and not config.WEB_ACCESS_TOKEN:
        print("WARNING: bound to a public interface with no WEB_ACCESS_TOKEN - "
              "anyone who can reach this port can submit orders. Set "
              "WEB_ACCESS_TOKEN in config.py before exposing to the internet.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
