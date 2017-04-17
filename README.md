# kafka-tool

## Installation
### Install from source
```bash
go get github.com/xuebing1110/kafka-tool
```

### Install from binary
[download](http://download.bingbaba.com/kafka-tool/)

## Usage
```bash
Usage of kafka-tool:
  -begin
        consume from begin
  -cmd string
        command (default "consumer")
  -filter string
        the filter when copy string
  -group string
        the consumer group
  -host string
        brokerlist/topic
  -partition int
        partitionnum (default -1)
  -print
        printflag
  -remote string
        brokerlist/topic, on using command "copy"
```

### Example
```bash
## consumer
kafka-tool -host 127.0.0.1:9092/mytopic

## consumer with filter
kafka-tool -host 127.0.0.1:9092/mytopic -filter aaaaaaaa

## consumer from begining
kafka-tool -host 127.0.0.1:9092/mytopic -begin

## copy topic to another topic
./kafka-tool -cmd copy -host 127.0.0.1:9092/mytopic1  -remote 127.0.0.1:9092/mytopic2

```