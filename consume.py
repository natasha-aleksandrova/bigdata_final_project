from google.cloud import pubsub_v1
import json
import time
import cql
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4
import os


GCP_PROJECT_ID = 'bigdata220-final-project'
TOPIC = "meetup_rsvp"
SUB_NAME = "out_meetup_rsvp"

print(os.environ)
cass_pass = os.environ['CASS_PASS']

auth_provider = PlainTextAuthProvider(username='cassandra', password=cass_pass)
cluster = Cluster(['10.138.0.2'], auth_provider=auth_provider)
session = cluster.connect('meetup')

def write_cassandra(message):
    print("Inserting to cassandra")
    try:
        print(type(message))
        r = session.execute("INSERT INTO rsvp_raw (id, blob) VALUES (%s, %s)", (uuid4(), message.data))    
        print(r)
    except Exception, e:
        print(e)

def receive_messages(project, subscription_name):
    """Receives messages from a pull subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        print('Received message: {}'.format(message))
        write_cassandra(message)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)



receive_messages(GCP_PROJECT_ID, SUB_NAME) 

