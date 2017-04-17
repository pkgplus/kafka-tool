package api

import (
	sarama "gopkg.in/Shopify/sarama.v1"
	cluster "gopkg.in/bsm/sarama-cluster.v2"
	"log"
)

func SetOffsetToNewest(brokers []string, group, topic string) error {
	// Init kafka client
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	kafka_client, client_err := cluster.NewClient(brokers, config)
	if client_err != nil {
		return client_err
	}

	//init cluster consumer with group
	cconsumer, err := cluster.NewConsumerFromClient(kafka_client, group, []string{topic})
	if err != nil {
		return err
	}
	go func() {
		for note := range cconsumer.Notifications() {
			log.Printf("kafka rebalanced:%v\n", note)
		}
	}()

	//init consumer for reading newest offset
	consumer, cerr := InitKafkaConsumer(brokers)
	if cerr != nil {
		return cerr
	}

	//partitions
	ps, perr := consumer.Partitions(topic)
	if perr != nil {
		cconsumer.Close()
		return perr
	}

	//read every partition newest offset and set it
	for _, pid := range ps {
		log.Printf("Read message from partition %d ...\n", pid)
		pconsumer, err := consumer.ConsumePartition(topic, pid, sarama.OffsetNewest)
		if err != nil {
			cconsumer.Close()
			log.Fatalf("Read message from partition %d err: %v", pid, err)
			continue
		}
		defer pconsumer.Close()

		msg := <-pconsumer.Messages()
		log.Printf("Reset %d partition offset %d\n", pid, msg.Offset)
		cconsumer.MarkOffset(msg, "kafka-tool-reset-offset")
	}

	// time.Sleep(time.Minute * 10)
	err3 := cconsumer.Close()
	if err3 != nil {
		return err3
	}

	for err := range cconsumer.Errors() {
		log.Printf("kafka consume error:%v\n", err)
	}

	return nil
}
