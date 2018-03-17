`ingest.py`

get meetup stream rvsp messages and write to google pubsub<br>

`pubsub_consume.py`

get messages out of google pubsub<br>
join rvsp data on category<br>
archive to cassandra<br>
write to kafka topic<br>

`streaming.py`

streaming spark app<br>
write to hdfs parquet file


`zeppelin.py`

aggregate / analysis spark app on the data<br>
