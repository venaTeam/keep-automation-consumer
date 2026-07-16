"""Prometheus metrics for the consumer (skeleton set)."""
from prometheus_client import Counter

messages_consumed = Counter(
    "keep_automation_consumer_messages_consumed_total",
    "Matched-alert messages consumed from the topic",
)
deserialize_errors = Counter(
    "keep_automation_consumer_deserialize_errors_total",
    "Matched-alert messages that failed to deserialize",
)
