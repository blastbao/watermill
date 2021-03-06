package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/blastbao/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/blastbao/watermill/message"
)

var (
	brokers = []string{"kafka:9092"}
	logger  = watermill.NewStdLogger(false, false)
)

type eventType string

const (
	Foo eventType = "Foo"
	Bar eventType = "Bar"
	Baz eventType = "Baz"
)

func main() {
	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	eventTypes := []eventType{Foo, Bar, Baz}

	for {
		eventType := eventTypes[rand.Intn(3)]
		msg := message.NewMessage(watermill.NewUUID(), []byte("message"))
		msg.Metadata.Set("event_type", string(eventType))

		fmt.Printf("%s Publishing %s\n\n", time.Now().String(), eventType)
		if err := pub.Publish("kafka_to_http_example", msg); err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}

}
