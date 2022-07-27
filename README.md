# msk demo

```linux
bin/kafka-topics.sh --create --topic demo-topic --partitions 16 --bootstrap-server b-1.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:9092,b-2.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:9092
```

```linux
bin/kafka-topics.sh --delete --topic demo-topic --bootstrap-server b-1.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:9092,b-2.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:9092
```
