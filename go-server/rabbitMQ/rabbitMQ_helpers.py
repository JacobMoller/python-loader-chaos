#!/usr/bin/env python
import os
import pika
import sys

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST")
if not RABBITMQ_HOST:
    raise EnvironmentError("RABBITMQ_HOST environment variable is not set.")
RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT")
if not RABBITMQ_PORT:
    raise EnvironmentError("RABBITMQ_PORT environment variable is not set.")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER")
if not RABBITMQ_USER:
    raise EnvironmentError("RABBITMQ_USER environment variable is not set.")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS")
if not RABBITMQ_PASS:
    raise EnvironmentError("RABBITMQ_PASS environment variable is not set.")
EXCHANGE_NAME = os.environ.get("RABBITMQ_EXCHANGE_NAME")
if not EXCHANGE_NAME:
    raise EnvironmentError("RABBITMQ_EXCHANGE_NAME environment variable is not set.")


def producer_connection_init():
    """
        Initialize the connection to RabbitMQ and return the channel and the connection.
    """
    
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=int(RABBITMQ_PORT), credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

    return channel, connection

def publish_message(channel, topic: str, body: str):
    channel.basic_publish(
    exchange=EXCHANGE_NAME, routing_key=topic, body=body)

def connection_end(connection, channel):
    """
        Close the connection to RabbitMQ.
    """
    channel.close()
    connection.close()

def listen(topic: str, callback):
    """
        :param str topic: The topic from which to consume
        :param callable on_message_callback: Required function to handle incoming messages, having the signature:
            on_message_callback(channel, method, properties, body)
            - channel: BlockingChannel
            - method: spec.Basic.Deliver
            - properties: spec.BasicProperties
            - body: bytes
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=int(RABBITMQ_PORT), credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(
        exchange=EXCHANGE_NAME, queue=queue_name, routing_key=topic)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()
    
