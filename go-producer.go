package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"

	example_person "./person"
)

const (
	kafkaConn = "localhost:9092"
	topic     = "person"
)

func main() {
	// create producer
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	// read command line input
	for i := 0; i < 2; i++ {
		elliot := &example_person.PersonMessage{
			Name: "Vijay",
			Age:  33,
		}

		data, err := proto.Marshal(elliot)
		if err != nil {
			log.Fatal("marshaling error: ", err)
		}

		// publish without goroutene
		publish(data, producer)

		// publish with go routene
		// go publish(msg, producer)
	}
}

func initProducer() (sarama.SyncProducer, error) {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	// async producer
	//prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config)

	// sync producer
	prd, err := sarama.NewSyncProducer([]string{kafkaConn}, config)

	return prd, err
}

func publish(message []byte, producer sarama.SyncProducer) {
	// publish sync
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}
