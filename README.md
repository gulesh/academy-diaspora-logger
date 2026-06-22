# academy-diaspora-logger

Academy `LogConfig` that ships agent logs to a [Diaspora](https://diaspora-project.org) Kafka topic, with built-in support for running agents on [Globus Compute](https://www.globus.org/compute).

## Installation

```bash
pip install academy-diaspora-logger
```

> **Globus Compute users:** install this package on **both** your local machine and the remote endpoint so that dill can deserialize `DiasporaLogConfig` on the worker.

## Usage

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
