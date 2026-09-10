"""All configurable parameters for the warehouse order-routing simulation."""

# --- Kafka connection --------------------------------------------------------
BOOTSTRAP_SERVERS = "localhost:9092"

# --- Topic layout ------------------------------------------------------------
TOPIC = "warehouse-orders"

# Warehouses, each owning one partition of the topic (the routing table).
WAREHOUSES = ["WH-BLR", "WH-DEL", "WH-MUM"]
WAREHOUSE_PARTITIONS = {wh: i for i, wh in enumerate(WAREHOUSES)}

# Priority orders bypass warehouse routing and land on a dedicated partition,
# read only by the priority processing application.
PRIORITY_PARTITION = len(WAREHOUSES)
NUM_PARTITIONS = len(WAREHOUSES) + 1

# Record header carrying the priority flag (value b"1" or b"0").
PRIORITY_HEADER = "x-priority"

# --- Simulation knobs --------------------------------------------------------
NUM_ORDERS = 20                       # orders the frontend submits per run
PRIORITY_RATIO = 0.25                 # fraction of orders flagged priority
ITEMS = ["laptop", "monitor", "keyboard", "dock", "headset", "cable"]

PRODUCE_DELAY_SEC = 0.2               # gap between order submissions
PROCESSING_TIME_SEC = 0.2             # simulated fulfilment work per order
PRIORITY_PROCESSING_TIME_SEC = 0.05   # expedited handling is faster

# --- Consumer / demo timing --------------------------------------------------
CONSUMER_POLL_TIMEOUT_MS = 1000
CONSUMER_START_GRACE_SEC = 3.0        # run_demo: wait for consumers to attach
DEMO_TIMEOUT_SEC = 60                 # run_demo: max wait for full consumption

# --- Web UI demo (webapp.py) ---------------------------------------------------
WEB_HOST = "0.0.0.0"                  # all interfaces; use "127.0.0.1" for local-only
WEB_PORT = 8000
WEB_BURST_SIZE = 10                   # orders sent by the "burst" button

# Required to submit orders when set (sent as the X-Demo-Token header; the UI
# prompts for it). ALWAYS set this before exposing the app to the internet.
WEB_ACCESS_TOKEN = ""
