package api

import (
	sarama "gopkg.in/Shopify/sarama.v1"
)

func InitKafkaProducer(brokers []string) (producer sarama.AsyncProducer, err error) {
	return sarama.NewAsyncProducer(brokers, nil)
}
