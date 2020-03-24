// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	"github.com/blastbao/watermill"
	"github.com/blastbao/watermill/message"
	"github.com/blastbao/watermill/pubsub/gochannel"
)

func main() {

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		watermill.NewStdLogger(false, false),
	)

	messages1, err := pubSub.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}

	go process(messages1)


	messages2, err := pubSub.Subscribe(context.Background(), "example.topic")
	if err != nil {
		panic(err)
	}
	go process(messages2)

	// build and publish messages at regular intervals
	publishMessages(pubSub)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))
		if err := publisher.Publish("example.topic", msg); err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
