== Bitmami Docker Kafka

=== Steps

 - Create Docker Network
 `docker network create kafka-net`

=== Connect shell

`docker exec -it kafka /bin/bash`

- Commands are located in directory `/opt/bitnami/kafka/bin/kafka`

- To execute a command from the host:
`docker exec -it bitnami_kafka_1 kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor3 --partitions 3 --topic mytopic`


