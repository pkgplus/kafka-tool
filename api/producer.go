package api

import (
	sarama "github.com/Shopify/sarama"
)

func InitKafkaProducer(brokers []string) (producer sarama.AsyncProducer, err error) {
	return sarama.NewAsyncProducer(brokers, nil)
}
