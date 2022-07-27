package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	zookeeper "github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
)

var (
	zk_addrs string
	addrs    []string
)

const (
	KAFKA_GROUP_NAME                   = "kafka_topic_push_group"
	OFFSETS_PROCESSING_TIMEOUT_SECONDS = 10 * time.Second
	OFFSETS_COMMIT_INTERVAL            = 10 * time.Second
)

func main() {
	flag.Parse()
	flag.StringVar(&zk_addrs, "z", "z-1.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:2181,z-3.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:2181,z-2.testmsk.15lq7p.c22.kafka.us-east-1.amazonaws.com:2181", "zookeeper address")
	addrs = strings.Split(zk_addrs, ",")
	if err := initProducer(); err != nil {
		logrus.Fatal(err)
	}
	logrus.Info("producer start")
	if err := initComsumer(); err != nil {
		logrus.Fatal(err)
	}
	logrus.Info("comsumer start")
	logrus.Info("demo start.")
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	<-c
}

var (
	producer sarama.AsyncProducer
)

func initProducer() (err error) {
	kafkaAddrs, err := GetKafkaAddrs()
	if err != nil {
		return
	}
	logrus.Info(kafkaAddrs)
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err = sarama.NewAsyncProducer(kafkaAddrs, config)
	go handleSuccess()
	go handleError()
	go producerInput()
	return
}

func handleSuccess() {
	var (
		pm *sarama.ProducerMessage
	)
	for {
		pm = <-producer.Successes()
		if pm != nil {
			logrus.Info("producer message success, partition:%d offset:%d key:%v values:%s", pm.Partition, pm.Offset, pm.Key, pm.Value)
		}
	}
}

func handleError() {
	var (
		err *sarama.ProducerError
	)
	for {
		err = <-producer.Errors()
		if err != nil {
			logrus.Error("producer message error, partition:%d offset:%d key:%v valus:%s error(%v)", err.Msg.Partition, err.Msg.Offset, err.Msg.Key, err.Msg.Value, err.Err)
		}
	}
}

func producerInput() {
	for i := 0; ; i++ {
		producer.Input() <- &sarama.ProducerMessage{Topic: "demo-topic", Value: sarama.StringEncoder(fmt.Sprintf("message %d", i))}
		time.Sleep(1 * time.Second)
	}
}

func initComsumer() (err error) {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = OFFSETS_PROCESSING_TIMEOUT_SECONDS
	config.Offsets.CommitInterval = OFFSETS_COMMIT_INTERVAL
	config.Zookeeper.Chroot = "/"
	kafkaTopics := []string{"demo-topic"}
	cg, err := consumergroup.JoinConsumerGroup(KAFKA_GROUP_NAME, kafkaTopics, addrs, config)
	if err != nil {
		return err
	}
	go func() {
		for err := range cg.Errors() {
			logrus.Error("consumer error(%v)", err)
		}
	}()
	go func() {
		for msg := range cg.Messages() {
			logrus.Info("deal with topic:%s, partitionId:%d, Offset:%d, Key:%s msg:%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			cg.CommitUpto(msg)
		}
	}()
	return
}

var (
	conn *zookeeper.Conn = nil
)

func GetKafkaAddrs() (kafkaAddrs []string, err error) {
	var (
		bytes []byte
		data  string
		datas []string
		dat   map[string]interface{}
		host  string
		port  string
	)
	if conn == nil {
		conn, _, err = zookeeper.Connect(addrs, 10*time.Second)
		if err != nil {
			return
		}
	}
	kafkaAddrs = make([]string, 0, 10)
	datas, _, err = conn.Children("/brokers/ids")
	if err != nil {
		logrus.Debug("zk ls fail")
		return
	}
	for _, data = range datas {
		bytes, _, err = conn.Get("/brokers/ids/" + data)
		if err != nil {
			logrus.Debug("zk get info fail")
			return
		}
		json.Unmarshal(bytes, &dat)
		for key, value := range dat {
			if key == "host" {
				host = value.(string)
			}
			if key == "port" {
				port = strconv.FormatFloat(value.(float64), 'f', 0, 64)
			}
		}
		kafkaAddrs = append(kafkaAddrs, host+":"+port)
	}
	return
}

func ChildrenW(path string) (addr []string, stat *zookeeper.Stat, event <-chan zookeeper.Event, err error) {
	if conn == nil {
		conn, _, err = zookeeper.Connect(addrs, 10*time.Second)
		if err != nil {
			return
		}
	}
	return conn.ChildrenW(path)
}
