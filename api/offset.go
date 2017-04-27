package api

import (
	sarama "gopkg.in/Shopify/sarama.v1"
	cluster "gopkg.in/bsm/sarama-cluster.v2"
	"log"
)

func GetNewestOffset(brokers []string, group, topic string) error {
	client, client_err := cluster.NewClient(brokers, nil)
	if client_err != nil {
		return client_err
	}
	defer client.Close()

	//get partitions
	parts, err := client.Partitions(topic)
	if err != nil {
		log.Printf("read %s partitions error:%v\n", topic, err)
		return err
	}
	for _, pid := range parts {
		broker, err := client.Leader(topic, pid)
		if err != nil {
			log.Printf("get %s[%d] broker error:%v\n", topic, pid, err)
			continue
		}

		//get newest offset
		oRequest := new(sarama.OffsetRequest)
		oRequest.AddBlock(topic, pid, sarama.OffsetNewest, 1)
		oResp, err := broker.GetAvailableOffsets(oRequest)
		if err != nil {
			log.Printf("get %s[%d] newest offset error:%v\n", topic, pid, err)
			continue
		}
		if oResp == nil {
			log.Printf("the %s[%d] newest offset response is null!\n", topic, pid)
			continue
		}

		//dump response
		for pid, block := range oResp.Blocks[topic] {
			log.Printf("%s\t%d\t%d\n", topic, pid, block.Offsets[0])
		}
	}

	return nil
}

func SetOffsetToNewest(brokers []string, group, topic string) error {
	// Init kafka client
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	kafka_client, client_err := cluster.NewClient(brokers, config)
	if client_err != nil {
		return client_err
	}
	defer kafka_client.Close()

	//init cluster consumer with group
	cconsumer, err := cluster.NewConsumerFromClient(kafka_client, group, []string{topic})
	if err != nil {
		return err
	}
	defer cconsumer.Close()
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
		return perr
	}

	//read every partition newest offset and set it
	for _, pid := range ps {
		log.Printf("Read message from partition %d ...\n", pid)
		pconsumer, err := consumer.ConsumePartition(topic, pid, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Read message from partition %d err: %v", pid, err)
			continue
		}

		msg := <-pconsumer.Messages()
		log.Printf("Reset %d partition offset %d\n", pid, msg.Offset)
		cconsumer.MarkOffset(msg, "kafka-tool-reset-offset")
	}

	for err := range cconsumer.Errors() {
		log.Printf("kafka consume error:%v\n", err)
	}

	return nil
}
