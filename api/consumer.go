package api

import (
	"fmt"
	sarama "github.com/Shopify/sarama"
	"log"
	"regexp"
	"sync"
)

func InitKafkaConsumer(brokers []string) (consumer sarama.Consumer, err error) {
	return sarama.NewConsumer(brokers, nil)
}

func StartConsumer(consumer sarama.Consumer, topic string, partition int, filterReg *regexp.Regexp, begin bool) error {
	//partitions
	ps, err := consumer.Partitions(topic)
	if err != nil {
		return err
	}

	//exit
	var wg sync.WaitGroup
	exitchan := getSignalChan(len(ps))

	//offset
	var offsettype int64
	if begin {
		offsettype = sarama.OffsetOldest
	} else {
		offsettype = sarama.OffsetNewest
	}

	for _, pid := range ps {
		//非指定partition
		if partition >= 0 && pid != int32(partition) {
			continue
		}

		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			pconsumer, err := consumer.ConsumePartition(topic, pid, offsettype)
			if err != nil {
				log.Fatalf("consume %s partition %d err: %v\n", topic, pid, err)
				return
			}

			var consumed int64
			var offset int64
		ConsumerLoop:
			for {
				select {
				case msg := <-pconsumer.Messages():
					body := string(msg.Value)
					if filterReg == nil || filterReg.MatchString(body) {
						offset = msg.Offset
						fmt.Printf("[%d] %s\n", pid, body)
						consumed++
					}
				case <-exitchan:
					break ConsumerLoop
				}
			}

			log.Printf("Consumed %d message over from %d partition ,offset is %d\n", consumed, pid, offset)
		}(pid)
	}

	wg.Wait()
	return nil
}
