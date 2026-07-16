"""Consumer constants (spec §2.3, §5.2; topic/group names per A0 / contracts)."""
from src.config.config import config

# Kafka — matched-alerts topic between event-handler (matcher) and this consumer.
KAFKA_BOOTSTRAP_SERVERS = config("KAFKA_BOOTSTRAP_SERVERS", default="localhost:29092")
MATCHED_ALERTS_TOPIC = config("MATCHED_ALERTS_TOPIC", default="matched-alerts")
KAFKA_CONSUMER_GROUP = config("KAFKA_CONSUMER_GROUP", default="keep-automation-consumer")
KAFKA_AUTO_OFFSET_RESET = config("KAFKA_AUTO_OFFSET_RESET", default="earliest")
KAFKA_POLL_TIMEOUT_SECONDS = config("KAFKA_POLL_TIMEOUT_SECONDS", default="1.0", cast=float)

# Worker pool sized toward ~200 submits/s (spec §2.3). Config knob only in the
# skeleton — the pool itself arrives with submit orchestration (C11).
WORKER_POOL_SIZE = config("WORKER_POOL_SIZE", default="200", cast=int)

# Ports (event-handler convention: health 8092, metrics 8094).
HEALTH_CHECK_PORT = config("HEALTH_CHECK_PORT", default="8092", cast=int)
PROMETHEUS_METRICS_PORT = config("PROMETHEUS_METRICS_PORT", default="8094", cast=int)
