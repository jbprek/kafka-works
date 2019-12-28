== Use of confluent docker images

https://docs.confluent.io/current/quickstart/cos-docker-quickstart.html[Quickstart]

=== Shell of broker image

Available commands under /usr/bin

`docker exec -it broker /bin/bash`

Available commands under /usr/bin

=== Create a topic
`docker exec broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic users`

=== List topics
`docker exec broker kafka-topics --list --zookeeper zookeeper:2181`

=== Delete topic
`docker  exec broker kafka-topics --delete --zookeeper zookeeper:2181 --topic users`