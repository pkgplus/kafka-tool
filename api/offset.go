package api

import (
	"log"
	// "math"

	sarama "github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func GetNewestOffset(brokers []string, topic string) ([]int64, error) {
	client, client_err := cluster.NewClient(brokers, nil)
	if client_err != nil {
		return nil, client_err
	}
	defer client.Close()

	//get partitions
	parts, err := client.Partitions(topic)
	if err != nil {
		log.Printf("read %s partitions error:%v\n", topic, err)
		return nil, err
	}

	offsets := make([]int64, len(parts))
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
			offsets[pid] = block.Offsets[0]
		}
	}

	return offsets, nil
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

func GetGroupOffset(brokers []string, group string) (map[string]map[int32]*sarama.OffsetFetchResponseBlock, error) {
	client, client_err := sarama.NewClient(brokers, nil)
	if client_err != nil {
		return nil, client_err
	}
	defer client.Close()

	result := make(map[string]map[int32]*sarama.OffsetFetchResponseBlock)
	for _, broker := range client.Brokers() {
		// log.Printf("connecting %s ...", broker.Addr())

		err := broker.Open(nil)
		if err != nil {
			log.Printf("ERR connect %s failed: %v", broker.Addr(), err)
			continue
		}

		fetch_req := &sarama.OffsetFetchRequest{
			ConsumerGroup: group,
			Version:       1,
		}
		resp, err := broker.FetchOffset(fetch_req)
		if err != nil {
			return nil, err
		}

		if len(resp.Blocks) > 0 {
			for topic, partitions := range resp.Blocks {
				data, found := result[topic]
				if found {
					for pid, offset_info := range partitions {
						data[pid] = offset_info
					}
				} else {
					result[topic] = partitions
				}
			}
		}
	}

	// topic = "__consumer_offsets"

	// // __consumer_offsets partitions
	// co_pids, err := consumer.Partitions(topic)
	// if err != nil {
	// 	return nil, err
	// }

	// // __consumer_offsets partition
	// co_pid := math.Abs(float64(HashCode(group)) % len(co_pids))
	// consumer.ConsumePartition(topic, co_pid, offset)
	return result, nil
}

func HashCode(in string) int32 {

	// Initialize output
	var hash int32

	// Empty string has a hashcode of 0
	if len(in) == 0 {
		return hash
	}

	// Convert string into slice of bytes
	b := []byte(in)

	// Build hash
	for i := range b {
		char := b[i]
		hash = ((hash << 5) - hash) + int32(char)
	}

	return hash
}
