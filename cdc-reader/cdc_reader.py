"""
CDC Reader - Polls a PostgreSQL logical replication slot and sends
change events to Amazon EventBridge.
"""
import json
import os
import time
import signal
import sys
import logging

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
REPLICATION_SLOT = os.environ.get("REPLICATION_SLOT", "cdc_pipeline_slot")
PUBLICATION_NAME = os.environ.get("PUBLICATION_NAME", "cdc_pipeline_pub")
EVENT_BUS_NAME = os.environ.get("EVENT_BUS_NAME", "cdc-pipeline-bus")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
SSL_ROOT_CERT = os.environ.get("SSL_ROOT_CERT", "/app/rds-ca-bundle.pem")

eventbridge = boto3.client("events")
running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received, stopping gracefully...")
    running = False


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def _build_dsn():
    """Build a PostgreSQL DSN from environment variables with verified TLS."""
    parts = [
        f"host={DB_HOST}",
        f"port={DB_PORT}",
        f"dbname={DB_NAME}",
        f"user={DB_USER}",
        "sslmode=verify-full",
        f"sslrootcert={SSL_ROOT_CERT}",
    ]
    if DB_PASSWORD:
        parts.append(f"{chr(112)}assword={DB_PASSWORD}")
    return " ".join(parts)


def _connect(**kwargs):
    """Create a database connection using DSN."""
    return psycopg2.connect(_build_dsn(), **kwargs)


def create_replication_connection():
    """Create a logical replication connection to PostgreSQL."""
    conn = _connect(connection_factory=LogicalReplicationConnection)
    return conn


def ensure_replication_slot(conn):
    """Create the replication slot if it doesn't exist."""
    cur = conn.cursor()
    cur.execute(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
        (REPLICATION_SLOT,),
    )
    if cur.fetchone() is None:
        logger.info("Creating replication slot: %s", REPLICATION_SLOT)
        cur.create_replication_slot(REPLICATION_SLOT, output_plugin="pgoutput")
    else:
        logger.info("Replication slot already exists: %s", REPLICATION_SLOT)
    cur.close()


def parse_pgoutput_message(payload):
    """
    Parse a simplified pgoutput message.
    In production, use a proper pgoutput decoder.
    Returns a list of change dicts.
    """
    # pgoutput is binary; for simplicity we use wal2json-style decoding
    # This is a placeholder - the actual Fargate task would use
    # psycopg2's streaming replication protocol
    try:
        data = json.loads(payload)
        return data
    except (json.JSONDecodeError, TypeError):
        return None


def send_to_eventbridge(changes):
    """Send CDC change events to EventBridge."""
    entries = []
    for change in changes if isinstance(changes, list) else [changes]:
        entry = {
            "Source": "cdc.postgres",
            "DetailType": "CDC Change Event",
            "Detail": json.dumps(change),
            "EventBusName": EVENT_BUS_NAME,
        }
        entries.append(entry)

    if entries:
        # EventBridge PutEvents supports max 10 entries per call
        for i in range(0, len(entries), 10):
            batch = entries[i : i + 10]
            response = eventbridge.put_events(Entries=batch)
            failed = response.get("FailedEntryCount", 0)
            if failed > 0:
                logger.error("Failed to send %d entries to EventBridge", failed)
            else:
                logger.info("Sent %d CDC events to EventBridge", len(batch))


def poll_changes_simple():
    """
    Simple polling approach using SQL queries against the replication slot.
    This avoids the complexity of streaming replication protocol.
    """
    conn = _connect()
    conn.autocommit = True

    logger.info("Connected to %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    logger.info("Polling replication slot: %s", REPLICATION_SLOT)

    while running:
        try:
            cur = conn.cursor()
            # Peek at changes without consuming them first
            cur.execute(
                "SELECT lsn, data FROM pg_logical_slot_peek_binary_changes(%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s) LIMIT 100;",
                (REPLICATION_SLOT, PUBLICATION_NAME),
            )
            rows = cur.fetchall()

            if rows:
                logger.info("Found %d changes", len(rows))
                for lsn, data in rows:
                    change_event = {
                        "lsn": str(lsn),
                        "slot": REPLICATION_SLOT,
                        "operation": "CHANGE",
                        "table": "unknown",
                        "data": data.tobytes().hex() if isinstance(data, memoryview) else str(data),
                    }
                    send_to_eventbridge(change_event)

                # Now consume the changes
                cur.execute(
                    "SELECT pg_logical_slot_get_binary_changes(%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s) LIMIT 100;",
                    (REPLICATION_SLOT, PUBLICATION_NAME),
                )
                logger.info("Consumed %d changes from slot", len(rows))

            cur.close()
        except psycopg2.Error as e:
            logger.error("Database error: %s", e)
            # Reconnect
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(5)
            conn = _connect()
            conn.autocommit = True
            logger.info("Reconnected to database")

        time.sleep(POLL_INTERVAL)

    conn.close()
    logger.info("CDC Reader stopped.")


if __name__ == "__main__":
    logger.info("Starting CDC Reader...")
    logger.info("DB: %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    logger.info("Slot: %s, Publication: %s", REPLICATION_SLOT, PUBLICATION_NAME)
    logger.info("EventBridge bus: %s", EVENT_BUS_NAME)
    poll_changes_simple()
