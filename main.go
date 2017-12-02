package main

import (
	"flag"
	kafkatool "github.com/xuebing1110/kafka-tool/api"
	"log"
	"os"
	"strings"
)

var (
	command, host, remote, filter, group *string
	ifPrint, begin                       *bool
	partition                            *int
)

func init() {
	command = flag.String("cmd", "consumer", "command, eg: consumer/copy/offset")
	host = flag.String("host", "", "brokerlist/topic")
	remote = flag.String("remote", "", "brokerlist/topic, on using command \"copy\"")
	filter = flag.String("filter", "", "the filter on copy string")
	group = flag.String("group", "", "the consumer group")
	ifPrint = flag.Bool("print", false, "printflag")
	begin = flag.Bool("begin", false, "consume from begin")
	partition = flag.Int("partition", -1, "partitionnum")
}

func main() {
	log.SetOutput(os.Stderr)
	flag.Parse()

	kafkaTool := &kafkatool.KafkaTool{
		Command:   *command,
		IfPrint:   *ifPrint,
		Begin:     *begin,
		Group:     *group,
		Partition: *partition,
		Filter:    *filter,
	}

	//host
	brokers_topic := strings.SplitN(*host, "/", 2)
	if len(brokers_topic) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	kafkaTool.Brokers = strings.Split(brokers_topic[0], ",")
	if len(brokers_topic) == 2 {
		kafkaTool.Topic = brokers_topic[1]
	}

	//remote
	if *remote != "" {
		dstbrokers_topic := strings.SplitN(*remote, "/", 2)
		if len(dstbrokers_topic) != 2 {
			flag.Usage()
			os.Exit(1)
		}
		kafkaTool.DstBrokers = strings.Split(dstbrokers_topic[0], ",")
		kafkaTool.DstTopic = dstbrokers_topic[1]
	}

	err := kafkaTool.Start()
	if err != nil {
		log.Fatalln(err)
	}
}
