# Kafka warehouse order-routing simulation

A frontend app submits orders to **one Kafka topic**; routing is content-based
and decided producer-side (the broker never inspects payloads):

- one partition per warehouse → consumed by that warehouse's fulfilment service
- one dedicated partition for priority orders → consumed **only** by the
  priority processing application
- every record carries an `x-priority` header

Same warehouse → same partition, so per-warehouse ordering is preserved
(message stickiness). Consumers use `assign()` instead of `subscribe()` so a
rebalance can never hand the priority partition to a warehouse consumer.

## Files

| file | role |
|---|---|
| `config.py` | all configurable parameters |
| `setup_topic.py` | creates the topic (warehouses + 1 partitions) |
| `producer.py` | frontend app — generates and routes orders |
| `consumer_base.py` | shared single-partition consume loop |
| `warehouse_consumer.py` | fulfilment service for one warehouse |
| `priority_consumer.py` | priority processing application |
| `run_demo.py` | end-to-end demo with summary + routing check |
| `webapp.py` + `web_ui.html` | web UI demo — watch routing live in the browser |

## Run

1. Start a broker (default `localhost:9092`, change in `config.py`):
   `docker compose up -d`
2. `pip install -r requirements.txt`
3. One-shot demo: `python run_demo.py`

Or interactively, one terminal each:

```
python setup_topic.py
python warehouse_consumer.py WH-BLR
python warehouse_consumer.py WH-DEL
python warehouse_consumer.py WH-MUM
python priority_consumer.py
python producer.py 30        # optional order count
```

## Web UI demo

```
python webapp.py        # then open http://127.0.0.1:8000
```

One process hosts the producer endpoint, all four pinned consumers, and a
Server-Sent Events stream. Buttons submit normal orders, priority orders, or a
random burst; the page shows each order land on its partition lane, get picked
up by the pinned consumer, and keeps a live routing check (priority orders must
only ever be consumed by the priority application). Host/port in `config.py`.
The web consumers use their own groups starting at `latest`, so old CLI-demo
traffic doesn't flood the page.


## Notes

- Warehouses/topic are edited in `config.py`. If the topic already exists with
  fewer partitions, delete it first (needs ≥ warehouses + 1 partitions).
- At larger scale you'd typically use a separate `orders.priority` topic
  instead of a partition; the producer-side content-based routing is identical.

## Containers 

1. The Kafka container , use the following Docker command for deploying Kafka container :
   `sudo docker run -d --name kafka -p 9092:9092 apache/kafka:latest`

2. To run Korvet (Ultra Streams) container:
   Korvet container depends on Redis Streams, hence needs Redis URL as an input parameter
   while launching the Korvet container. IN case of local environment, deploy the Redis container first,
   followed by Korvet container.
   
   `sudo docker run -d  -p 6379:6379 redis:latest`
   
   `sudo docker run -d -p 9092:9092 -p 8080:8080   -e KORVET_REDIS_URI=redis://LocalIP:6379   redisfield/korvet `
   
   ```
   Specify local ip since specifying `redis://localhost:6379`, will result in Korvet container looking for redis within the self docker container. 
   ```
