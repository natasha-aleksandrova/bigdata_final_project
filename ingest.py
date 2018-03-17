import json
import pycurl
from google.cloud import pubsub_v1

STREAM_URL = "http://stream.meetup.com/2/rsvps"
GCP_PROJECT_ID = 'bigdata220-final-project'
TOPIC = "meetup_rsvp"
SUBSCRIPTION = "out_meetup_rsvp"


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC)


def on_receive(data):
    print("Received new RSVP: %s" % data)
    publisher.publish(topic_path, data=data)


conn = pycurl.Curl()
conn.setopt(pycurl.URL, STREAM_URL)
conn.setopt(pycurl.WRITEFUNCTION, on_receive)
conn.perform()
