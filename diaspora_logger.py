"""Diaspora event-fabric logging handler for Academy agents.

Provides :class:`DiasporaLogConfig` — an Academy ``LogConfig`` that ships
every log record to a Diaspora/Kafka topic — and :class:`DiasporaHandler`,
the underlying ``logging.Handler``.

Quickstart (local execution)::

    from diaspora_logger import DiasporaLogConfig
    log_cfg = DiasporaLogConfig(kafka_topic)

Quickstart (Globus Compute / remote execution)::

    from diaspora_logger import DiasporaLogConfig
    # Call prefetch() on the LOCAL machine before launching agents.
    # It fetches AWS credentials using your local Globus auth so the
    # remote worker never needs Globus tokens.
    log_cfg = DiasporaLogConfig.prefetch(kafka_topic)

    # Pass log_cfg to the Academy Manager as normal.
    # IMPORTANT: this package must also be pip-installed on the remote
    # Globus Compute endpoint so dill can deserialise DiasporaLogConfig.
"""

from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Callable
from typing import Any

from academy.logging.configs.base import LogConfig

DEFAULT_FORMAT = (
    "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d "
    "%(process)d %(threadName)s [%(levelname)s] %(message)s"
)


def resolve_kafka_topic(topic_name: str, namespace: str) -> str:
    """Expand a short topic name to ``namespace.topic`` if not already qualified."""
    return topic_name if "." in topic_name else f"{namespace}.{topic_name}"


def get_ssl_cafile() -> str | None:
    """Return certifi's CA bundle path if available, else ``None``."""
    try:
        import certifi
        return certifi.where()
    except ImportError:
        return None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return repr(value)


class DiasporaHandler(logging.Handler):
    """Logging handler that publishes records to a Diaspora Kafka topic."""

    def __init__(self, producer: Any, kafka_topic: str, send_timeout: int = 30) -> None:
        super().__init__()
        self.producer = producer
        self.kafka_topic = kafka_topic
        self.send_timeout = send_timeout

    def emit(self, record: logging.LogRecord) -> None:
        try:
            formatted = self.format(record) if self.formatter is not None else None
            event = {k: _json_safe(v) for k, v in record.__dict__.items()}
            event["message"] = record.getMessage()
            if formatted is not None:
                event["formatted"] = formatted
            self.producer.send(self.kafka_topic, event)
        except Exception:  # noqa: BLE001
            self.handleError(record)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.producer.flush(timeout=self.send_timeout)
        super().close()


class DiasporaLogConfig(LogConfig):
    """Academy ``LogConfig`` that ships logs to a Diaspora Kafka topic.

    The ``KafkaProducer`` is created inside :meth:`init_logging` so the
    config object stays pickleable and can be sent to remote workers.

    For **local execution** (threads/processes on the same machine)::

        log_cfg = DiasporaLogConfig(kafka_topic)

    For **Globus Compute** (or any remote executor without Globus tokens)::

        log_cfg = DiasporaLogConfig.prefetch(kafka_topic)

    ``prefetch`` fetches AWS IAM credentials on the local machine using your
    cached Globus auth, embeds them in the config, and the remote worker uses
    them directly — no Globus auth needed on the worker side.

    This package must be installed on the remote endpoint::

        pip install academy-diaspora-logger   # on the endpoint machine
    """

    def __init__(
        self,
        kafka_topic: str,
        logger_name: str = "academy",
        level: int = logging.DEBUG,
        send_timeout: int = 30,
        max_block_ms: int = 1000,
        _credentials: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.kafka_topic = kafka_topic
        self.logger_name = logger_name
        self.level = level
        self.send_timeout = send_timeout
        self.max_block_ms = max_block_ms
        self._credentials = _credentials

    @classmethod
    def prefetch(
        cls,
        kafka_topic: str,
        logger_name: str = "academy",
        level: int = logging.DEBUG,
        send_timeout: int = 30,
        max_block_ms: int = 1000,
    ) -> "DiasporaLogConfig":
        """Fetch Kafka credentials now and embed them in the config.

        Call this on the **local machine** before launching agents on Globus
        Compute.  It uses your locally cached Globus auth to retrieve AWS IAM
        credentials from the Diaspora service.  Those credentials travel with
        the pickled config to the remote worker, so ``init_logging()`` can
        connect to Kafka without needing Globus tokens on the remote side.

        Args:
            kafka_topic: Fully-qualified topic name (``namespace.topic``).
            logger_name: Root logger name to attach the handler to.
            level: Logging level for the handler.
            send_timeout: Seconds to wait when flushing the Kafka producer.
            max_block_ms: Max milliseconds to block on producer send.

        Returns:
            A ``DiasporaLogConfig`` with embedded AWS credentials ready to
            be sent to a remote Globus Compute worker.
        """
        from diaspora_event_sdk import Client as GlobusClient

        client = GlobusClient()
        keys = client.create_key()
        credentials = {
            "access_key": keys["access_key"],
            "secret_key": keys["secret_key"],
            "endpoint": keys["endpoint"],
        }
        return cls(
            kafka_topic,
            logger_name=logger_name,
            level=level,
            send_timeout=send_timeout,
            max_block_ms=max_block_ms,
            _credentials=credentials,
        )

    def init_logging(self) -> Callable[[], None]:
        ssl_cafile = get_ssl_cafile()

        if self._credentials is not None:
            # Remote path: use pre-fetched AWS IAM credentials so we never
            # call Client().create_key() on the worker (requires Globus auth).
            import os

            from diaspora_event_sdk.sdk.aws_iam_msk import generate_auth_token

            try:
                from kafka import KafkaProducer as _KafkaProducer
                from kafka.sasl.oauth import AbstractTokenProvider
            except ImportError as exc:
                raise RuntimeError(
                    "kafka-python is required. Install with: pip install 'kafka-python<3'",
                ) from exc

            os.environ["OCTOPUS_AWS_ACCESS_KEY_ID"] = self._credentials["access_key"]
            os.environ["OCTOPUS_AWS_SECRET_ACCESS_KEY"] = self._credentials["secret_key"]
            os.environ["OCTOPUS_BOOTSTRAP_SERVERS"] = self._credentials["endpoint"]

            class _MSKTokenProvider(AbstractTokenProvider):
                def token(self) -> str:
                    token, _ = generate_auth_token("us-east-1")
                    return token

            producer_kwargs: dict[str, Any] = {
                "bootstrap_servers": self._credentials["endpoint"],
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "api_version": (3, 8, 1),
                "sasl_oauth_token_provider": _MSKTokenProvider(),
                "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
                "max_block_ms": self.max_block_ms,
            }
            if ssl_cafile:
                producer_kwargs["ssl_cafile"] = ssl_cafile
            producer = _KafkaProducer(**producer_kwargs)
        else:
            # Local path: KafkaProducer handles auth via Globus tokens on disk.
            from diaspora_event_sdk.sdk.kafka_client import KafkaProducer

            local_kwargs: dict[str, Any] = {"max_block_ms": self.max_block_ms}
            if ssl_cafile:
                local_kwargs["ssl_cafile"] = ssl_cafile
            producer = KafkaProducer(self.kafka_topic, **local_kwargs)

        handler = DiasporaHandler(producer, self.kafka_topic, self.send_timeout)
        handler.setLevel(self.level)
        handler.setFormatter(logging.Formatter(DEFAULT_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))

        target = logging.getLogger(self.logger_name)
        target.setLevel(logging.DEBUG)
        target.addHandler(handler)

        def uninitialize() -> None:
            target.removeHandler(handler)
            with contextlib.suppress(Exception):
                producer.close(timeout=self.send_timeout)
            with contextlib.suppress(Exception):
                handler.close()

        return uninitialize


__all__ = [
    "DEFAULT_FORMAT",
    "DiasporaHandler",
    "DiasporaLogConfig",
    "get_ssl_cafile",
    "resolve_kafka_topic",
]
