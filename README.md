`ingest.py`

get meetup stream rvsp messages and write to google pubsub

`pubsub_consume.py`

get messages out of google pubsub
join rvsp data on category
archive to cassandra
write to kafka topic

`streaming.py`

streaming spark app
write to hdfs parquet file


`zeppelin.py`

aggregate / analysis spark app on the data
