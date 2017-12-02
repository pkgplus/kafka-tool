package api

import (
	"errors"
	sarama "github.com/Shopify/sarama"
	"log"
	"regexp"
)

const (
	CMD_CONSUMER    = "consumer"
	CMD_COPY        = "copy"
	CMD_RESETOFFSET = "resetOffset"
	CMD_OFFSET      = "offset"
)

var (
	ERR_MISSING_HOST   = errors.New("brokerlist or topic is empty, pls set it with \"host\" parameter")
	ERR_MISSING_REMOTE = errors.New("dstbrokerlist or dsttopic is empty, pls set it with \"remote\" parameter")
	ERR_MISSING_GROUP  = errors.New("group is empty, pls set it with \"group\" parameter")
)

type KafkaTool struct {
	Command    string
	IfPrint    bool
	Begin      bool
	Brokers    []string
	DstBrokers []string
	Group      string
	Topic      string
	DstTopic   string
	Partition  int
	Filter     string

	filterReg *regexp.Regexp
	consumer  sarama.Consumer
	producer  sarama.AsyncProducer
}

func (k *KafkaTool) Init() (err error) {
	if k.Filter != "" {
		k.filterReg, err = regexp.Compile(k.Filter)
		if err != nil {
			return
		}
	}
	return
}

func (k *KafkaTool) Start() (err error) {
	err = k.Init()
	if err != nil {
		return
	}

	switch k.Command {
	case CMD_CONSUMER:
		err = k.StartKafkaConsumer()
	case CMD_COPY:
		err = k.StartTopicCopy()
	case CMD_RESETOFFSET:
		err = k.SetOffsetToNewest()
	case CMD_OFFSET:
		// err = k.GetNewestOffset()
		err = k.GetGroupOffset()
	default:
		err = errors.New("unknown command: " + k.Command)
	}

	return
}

func (k *KafkaTool) StartKafkaConsumer() (err error) {
	//check
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	k.consumer, err = InitKafkaConsumer(k.Brokers)
	if err != nil {
		return
	}
	defer k.consumer.Close()

	return StartConsumer(k.consumer, k.Topic, k.Partition, k.filterReg, k.Begin)
}

func (k *KafkaTool) StartTopicCopy() (err error) {
	//check
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	if len(k.DstBrokers) == 0 || k.DstTopic == "" {
		return ERR_MISSING_REMOTE
	}

	//init
	k.consumer, k.producer, err = InitKafkaCopy(k.Brokers, k.DstBrokers)
	if err != nil {
		return
	}
	defer k.consumer.Close()
	defer k.producer.Close()

	//start
	err = StartTopicCopy(k.consumer, k.producer, k.Topic, k.DstTopic, k.IfPrint, k.filterReg, k.Begin)
	if err != nil {
		return
	}

	return
}

func (k *KafkaTool) SetOffsetToNewest() error {
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}
	if k.Group == "" {
		return ERR_MISSING_GROUP
	}

	return SetOffsetToNewest(k.Brokers, k.Group, k.Topic)
}

func (k *KafkaTool) GetNewestOffset() error {
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	offsets, err := GetNewestOffset(k.Brokers, k.Topic)
	if err != nil {
		return err
	}

	for pid, offset := range offsets {
		log.Printf("%s\t%d\t%d\n", k.Topic, pid, offset)
	}

	return nil
}

func (k *KafkaTool) GetGroupOffset() error {
	if len(k.Brokers) == 0 {
		return ERR_MISSING_HOST
	}

	if k.Group == "" {
		return ERR_MISSING_GROUP
	}

	offsets, err := GetGroupOffset(k.Brokers, k.Group)
	if err != nil {
		return err
	}

	log.Printf("GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-END-OFFSET\tLAG\tOWNER")
	for topic, partitions := range offsets {
		newest_offsets, err := GetNewestOffset(k.Brokers, topic)
		if err != nil {
			return err
		}

		for id, newest_offset := range newest_offsets {
			pid := int32(id)
			current_offset := partitions[pid].Offset
			lag := newest_offset - current_offset

			owner := partitions[pid].Metadata
			if owner == "" {
				owner = "NULL"
			}

			log.Printf("%s\t%s\t%d\t%d\t%d\t%d\t%s", k.Group, topic, pid, current_offset, newest_offset, lag, owner)
		}
	}

	return nil
}
