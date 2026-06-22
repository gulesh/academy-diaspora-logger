# academy-diaspora-logger

Academy `LogConfig` that ships agent logs to a [Diaspora](https://diaspora-project.org) Kafka topic, with built-in support for running agents on [Globus Compute](https://www.globus.org/compute).

Also ships a `diaspora` CLI for managing credentials, fetching events, and clearing topics.

## Installation

```bash
pip install academy-diaspora-logger
```

> **Globus Compute users:** install this package on **both** your local machine and the remote endpoint so that dill can deserialize `DiasporaLogConfig` on the worker.

---

## Python API

### Local execution (threads / local processes)

```python
from diaspora_logger import DiasporaLogConfig
from academy.manager import Manager
from academy.exchange import LocalExchangeFactory
from concurrent.futures import ThreadPoolExecutor

log_cfg = DiasporaLogConfig(kafka_topic)

async with await Manager.from_exchange_factory(
    factory=LocalExchangeFactory(),
    executors=ThreadPoolExecutor(max_workers=3),
    log_config=log_cfg,
) as manager:
    ...
```

### Globus Compute (remote execution)

Call `prefetch()` on your **local machine** before launching agents. It uses your locally cached Globus credentials to fetch AWS IAM keys from the Diaspora service and embeds them in the config. The remote worker receives the keys via the pickled config and never needs Globus auth itself.

```python
from diaspora_logger import DiasporaLogConfig
from academy.manager import Manager
from academy.exchange.cloud.client import HttpExchangeFactory
from globus_compute_sdk import Executor as GCExecutor

log_cfg = DiasporaLogConfig.prefetch(kafka_topic)

async with await Manager.from_exchange_factory(
    factory=HttpExchangeFactory(),
    executors=GCExecutor(endpoint_id),
    log_config=log_cfg,
) as manager:
    ...
```

### Consuming events locally

```python
from diaspora_context import get_diaspora_events

result = get_diaspora_events(
    topic_name=kafka_topic,
    time_horizon=time_before_ms,   # unix epoch milliseconds
)
for event in result["events"]:
    print(event)
```

---

## CLI

After installation a `diaspora` command is available on your PATH.

### `diaspora setup`

Register your Globus identity with Diaspora and create your IAM credentials.

```bash
diaspora setup
```

### `diaspora context`

Fetch recent events from a topic and print them as JSON.

```bash
diaspora context --topic my-topic
diaspora context --topic ns-abc123.my-topic --lookback 7200   # last 2 hours
diaspora context --topic my-topic --time-horizon 1718000000000 --max-messages 500
```

| Flag | Default | Description |
|---|---|---|
| `--topic` | required | Topic name (short or fully-qualified) |
| `--lookback` | `3600` | Seconds to look back from now |
| `--time-horizon` | — | Unix-epoch ms timestamp (overrides `--lookback`) |
| `--timeout-ms` | `30000` | Consumer poll timeout |
| `--max-messages` | `10000` | Maximum events to return |

### `diaspora clear`

Recreate (wipe) a topic.

```bash
diaspora clear my-topic
```

---

## How it works

| Scenario | Auth | How credentials reach Kafka |
|---|---|---|
| Local | Globus tokens on disk | `KafkaProducer` calls `Client().create_key()` automatically |
| Globus Compute | `prefetch()` called locally once | AWS IAM keys embedded in pickled config, used directly on worker |

`prefetch()` calls `Client().create_key()` once on the local machine (where Globus tokens are cached) and stores the returned `access_key`, `secret_key`, and `endpoint` in the config object. When Academy pickles the config and ships it to the remote worker, those credentials travel with it. `init_logging()` on the remote then builds a raw `kafka.KafkaProducer` from those credentials — no Globus auth needed on the worker side.

## Requirements

- Python ≥ 3.10
- `academy-py >= 0.3.1`
- `diaspora-event-sdk`
- `kafka-python < 3`
- `certifi` (optional, recommended for macOS SSL)
