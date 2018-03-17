from google.cloud import pubsub_v1
import json
import time
import cql
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4
import os
from kafka import KafkaProducer
import requests


GCP_PROJECT_ID = 'bigdata220-final-project'
TOPIC = "meetup_rsvp"
SUBSCRIPTION = "out_meetup_rsvp"

CASS_PASS = os.environ['CASS_PASS']
auth_provider = PlainTextAuthProvider(username='cassandra', password=CASS_PASS)
cluster = Cluster(['10.138.0.2'], auth_provider=auth_provider)

KAFKA_TOPIC = "rsvps2"
kafka_producer = KafkaProducer(bootstrap_servers='10.128.0.8:9092')


def join_with_category(message):
    message = json.loads(message)
    try:
        group_urlname = message['group']['group_urlname']
    except Exception as e:
        print(e)

    print("Joining on %s" % group_urlname)
    response = requests.get(
        "https://api.meetup.com/{group_urlname}".format(group_urlname=group_urlname))

    if response.status_code != 200:
        print("Failed joining on category: %s" % e)
    else:
        message["category"] = response.json()["category"]

    return json.dumps(message)


def write_cassandra(message):
    print("Inserting to cassandra: %s" % type(message))
    try:
        session = cluster.connect('meetup')
        r = session.execute("INSERT INTO rsvp_raw (id, blob) VALUES (%s, %s)", (uuid4(), message))
    except Exception as e:
        print("Failed writing to cassandra: %s" % e)


def send_to_kafka(message):
    print("sending to kafka")
    future = kafka_producer.send(KAFKA_TOPIC, message)
    print(future.get(timeout=60))
    print("sent to kafka!")


def receive_messages(project, subscription_name):
    """Receives messages from a pull subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message_obj):
        print('Consumed message: {}'.format(message_obj))

        message = join_with_category(message_obj.data)

        write_cassandra(message)

        send_to_kafka(message)

        message_obj.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)


receive_messages(GCP_PROJECT_ID, SUBSCRIPTION)
