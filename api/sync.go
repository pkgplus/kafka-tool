package api

import (
	"fmt"
	sarama "gopkg.in/Shopify/sarama.v1"
	"log"
	"regexp"
	"sync"
)

func InitKafkaCopy(brokers, dstbrokers []string) (consumer sarama.Consumer, producer sarama.AsyncProducer, err error) {
	//consumer
	consumer, err = InitKafkaConsumer(brokers)
	if err != nil {
		return
	}

	//producer
	producer, err = InitKafkaProducer(dstbrokers)
	if err != nil {
		return
	}

	return
}

func StartTopicCopy(consumer sarama.Consumer, producer sarama.AsyncProducer, topic, dsttopic string, ifPrint bool, reg *regexp.Regexp, begin bool) error {
	ps, err := consumer.Partitions(topic)
	if err != nil {
		return err
	}

	//exit signal
	exitchan := getSignalChan(len(ps))

	//offset
	var offsettype int64
	if begin {
		offsettype = sarama.OffsetOldest
	} else {
		offsettype = sarama.OffsetNewest
	}

	//every partition
	var wg sync.WaitGroup
	for _, pid := range ps {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()

			pconsumer, err := consumer.ConsumePartition(topic, pid, offsettype)
			if err != nil {
				log.Fatalf("consumer %s partition %d err: %v", topic, pid, err)
				return
			}

			var consumed, enqueued, offset int64
		ConsumerLoop:
			for {
				select {
				case msg := <-pconsumer.Messages():
					body := string(msg.Value)

					if reg == nil || reg.MatchString(body) {
						if ifPrint {
							fmt.Println(body)
						}

						producer.Input() <- &sarama.ProducerMessage{
							Topic: dsttopic,
							Key:   nil,
							Value: sarama.StringEncoder(body),
						}
						enqueued++
					}

					offset = msg.Offset
					consumed++
				case <-exitchan:
					break ConsumerLoop
				}
			}

			log.Printf("Consumed %d message over from %d partition,send %d message to %s ,offset is %d\n",
				consumed, pid, enqueued,
				dsttopic, offset)
		}(pid)
	}

	wg.Wait()

	return nil
}
