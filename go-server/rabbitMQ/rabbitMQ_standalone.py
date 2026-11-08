"""
Helpers for the "standalone" plugin architecture.

Every standalone plugin owns one durable, named queue (see queue_name) that it
declares itself and binds to the shared topic exchange. Because each plugin has
its own queue, every message published on the exchange is fanned out to all
plugins whose bindings match, while replicas of the same plugin compete for the
messages of that one queue.

Producers are unchanged: they keep publishing to the exchange with a routing
key (see publish) and never need to know which queues exist.
"""
import functools
import logging
import queue
import threading
import time

import pika

from rabbitMQ_helpers import (
    EXCHANGE_NAME,
    RABBITMQ_HOST,
    RABBITMQ_PASS,
    RABBITMQ_PORT,
    RABBITMQ_USER,
)

logger = logging.getLogger(__name__)

QUEUE_PREFIX = "standalone."
RECONNECT_DELAY = 5  # seconds


def queue_name(plugin: str) -> str:
    """Name of the queue owned by the given plugin."""
    return QUEUE_PREFIX + plugin


# Set by consume(), used to name this plugin's connections
_plugin_queue = QUEUE_PREFIX + "unknown"


def _connect(role: str, heartbeat=60):
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=int(RABBITMQ_PORT),
            credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
            heartbeat=heartbeat,
            blocked_connection_timeout=300,
            # Shown in the management UI instead of only the client's ip:port
            client_properties={"connection_name": f"{_plugin_queue} {role}"},
        )
    )


def declare_plugin_queue(channel, name: str, binding_keys: list[str]):
    """Declare the exchange, the plugin's durable queue and its bindings.
    Declarations are idempotent, so this is safe to run on every start."""
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    # durable: survives plugin restarts, messages wait for the plugin
    # not exclusive: replicas of the plugin share the queue
    channel.queue_declare(queue=name, durable=True)
    for key in binding_keys:
        logger.info("Binding queue %s to exchange %s with routing key %s", name, EXCHANGE_NAME, key)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=name, routing_key=key)


# =========================
# CONSUMING
# =========================
def consume(plugin: str, binding_keys: list[str], handler):
    """
    Declare the plugin's queue and call handler(body: bytes, routing_key: str)
    for each message, one at a time. Blocks forever, reconnecting when needed.

    The handler always runs on the same worker thread, so it may keep
    thread-bound state between messages. Meanwhile the connection thread keeps
    answering heartbeats, so handlers can run for a long time (GPU inference)
    without RabbitMQ closing the connection.

    A message is acknowledged once the handler returns. If the handler raises,
    the error is logged and the message is dropped (not requeued, so a message
    that always fails cannot block the queue). If the connection is lost while
    the handler runs, the message is redelivered.
    """
    global _plugin_queue
    name = _plugin_queue = queue_name(plugin)
    work: queue.Queue = queue.Queue()
    threading.Thread(
        target=_worker, args=(work, handler), name=f"{plugin}-worker", daemon=True
    ).start()

    while True:
        try:
            connection = _connect("consumer")
            channel = connection.channel()
            declare_plugin_queue(channel, name, binding_keys)
            # Only hand one unacknowledged message at a time to this consumer,
            # the rest stay in the queue (and go to other replicas, if any)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=name,
                on_message_callback=functools.partial(_on_message, connection, work),
            )
            logger.info(" [*] Consuming from %s", name)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("Connection to RabbitMQ lost (%s), reconnecting in %ds", e, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)


def _on_message(connection, work, channel, method, properties, body):
    # Runs on the connection thread: hand the message to the worker and return
    # immediately so the connection keeps processing heartbeats
    work.put((connection, channel, method, body))


def _worker(work: queue.Queue, handler):
    while True:
        connection, channel, method, body = work.get()
        ok = True
        try:
            handler(body, method.routing_key)
        except Exception:
            ok = False
            logger.exception("Handler failed for message on %s, dropping it", method.routing_key)

        # Channels are not thread-safe, the ack must happen on the connection thread
        settle = functools.partial(_settle, channel, method.delivery_tag, ok)
        try:
            connection.add_callback_threadsafe(settle)
        except Exception as e:
            logger.warning("Could not settle message (connection lost, it will be redelivered): %s", e)


def _settle(channel, delivery_tag, ok):
    if not channel.is_open:
        return
    if ok:
        channel.basic_ack(delivery_tag=delivery_tag)
    else:
        channel.basic_nack(delivery_tag=delivery_tag, requeue=False)


# =========================
# PUBLISHING
# =========================
_pub_lock = threading.Lock()
_pub_connection = None
_pub_channel = None


def publish(routing_key: str, body: str):
    """Publish a message on the exchange. Thread-safe, reconnects if the
    publish connection was closed (e.g. by RabbitMQ after being idle)."""
    global _pub_connection, _pub_channel
    with _pub_lock:
        for attempt in (1, 2):
            try:
                if _pub_channel is None or _pub_channel.is_closed:
                    # No heartbeats: a blocking connection only answers them while
                    # publishing, so an idle one would be closed by RabbitMQ
                    _pub_connection = _connect("publisher", heartbeat=0)
                    _pub_channel = _pub_connection.channel()
                    _pub_channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
                _pub_channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=body)
                return
            except pika.exceptions.AMQPError as e:
                if attempt == 2:
                    raise
                logger.warning("Publish on %s failed (%s), reconnecting...", routing_key, e)
                try:
                    _pub_connection.close()
                except Exception:
                    pass
                _pub_channel = None
