#!/usr/bin/env python
import pika
from dotenv import load_dotenv
import os
from modules.sd_models import CheckpointInfo, load_model, model_data
from modules.timer import Timer

load_dotenv()
parameters = pika.URLParameters(os.getenv("AMQP_URL"))
connection = pika.BlockingConnection(parameters)

MQ_CHANNEL = connection.channel()
MQ_CHANNEL.basic_qos(prefetch_count=1)

MQ_CHANNEL = connection.channel()
MQ_CHANNEL = connection.channel()

queue_tags = {}


def get_queues():
    return


def bind_consume(queue, process_image_request):
    filename = "./models/Stable-diffusion/" + queue + ".safetensors"
    checkpoint_info = CheckpointInfo(filename)

    model_data.sd_model = None
    load_model(checkpoint_info)

    MQ_CHANNEL.queue_declare(queue=queue, durable=True)
    print(" [*] Waiting for messages in {}. To exit press CTRL+C".format(queue))
    tag = MQ_CHANNEL.basic_consume(
        queue=queue, on_message_callback=process_image_request
    )
    queue_tags[queue] = tag


def cancel_consume(queue):
    MQ_CHANNEL.stop_consuming()
    MQ_CHANNEL.basic_cancel(queue_tags[queue])
    MQ_CHANNEL.start_consuming()

    def on_open(connection):
        connection.channel(on_open_callback=on_channel_open)

    def on_channel_open(channel):
        channel.basic_publish(
            "test_exchange",
            "test_routing_key",
            "message body value",
            pika.BasicProperties(
                content_type="text/plain", delivery_mode=pika.DeliveryMode.Transient
            ),
        )
        connection.close()

    parameters = pika.URLParameters("amqp://guest:guest@localhost:5672/%2F")
    connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)


def stop_consume(queue):
    MQ_CHANNEL.stop_consuming(queue=queue)


def start_consume():
    MQ_CHANNEL.start_consuming()
