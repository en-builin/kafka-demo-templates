# Kafka Demo Examples and Templates

Plain demonstrations of usage Kafka Clients APIs in Java Gradle project.

Each class contains simple main method to run and test different behaviors of Kafka Producers and Consumers

# How to use

Install Apache Kafka locally on standard port 9092 or use Docker Compose in project root:

```shell
docker compose up
```

Create topic with your favourite Kafka tool, or with CLI tools from Docker Container SSH:

```shell
docker exec -it broker /usr/bin/bash
cd /usr/bin
kafka-topic --bootstrap-server localhost:9092 --create --topic test_topic --partitions 3 --replication-factor 1
```

Open project in IntelliJ or any other IDE and run every demo class you want to test. Use classes code as snippets for your projects.
