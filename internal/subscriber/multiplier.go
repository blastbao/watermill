package subscriber

import (
	"context"
	"sync"

	"github.com/blastbao/watermill/message"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type Constructor func() (message.Subscriber, error)

type multiplier struct {
	subscriberConstructor func() (message.Subscriber, error)
	subscribersCount      int
	subscribers           []message.Subscriber
}

// NewMultiplier returns multiplier subscriber decorator,
// which under the hood is calling subscribe multiple times to increase throughput.
func NewMultiplier(constructor Constructor, subscribersCount int) message.Subscriber {
	return &multiplier{
		subscriberConstructor: constructor,
		subscribersCount:      subscribersCount,
	}
}

// [important]
func (s *multiplier) Subscribe(ctx context.Context, topic string) (msgs <-chan *message.Message, err error) {

	defer func() {
		if err != nil {
			if closeErr := s.Close(); closeErr != nil {
				err = multierror.Append(err, closeErr)
			}
		}
	}()

	out := make(chan *message.Message)

	subWg := sync.WaitGroup{}
	subWg.Add(s.subscribersCount)

	// for loop => create some subscribers of topic and transfer the message to channel out
	for i := 0; i < s.subscribersCount; i++ {

		// make a subscriber
		sub, err := s.subscriberConstructor()
		if err != nil {
			return nil, errors.Wrap(err, "cannot create subscriber")
		}

		// save the subscriber
		s.subscribers = append(s.subscribers, sub)

		// call subscriber.Subscribe(topic) to get a message channel for receiving messages of topic.
		msgs, err := sub.Subscribe(ctx, topic)
		if err != nil {
			return nil, errors.Wrap(err, "cannot subscribe")
		}

		// create a backend goroutine receiving messages from topic then send to channel out.
		go func() {
			for msg := range msgs {
				out <- msg
			}
			subWg.Done()
		}()
	}

	go func() {
		subWg.Wait()
		close(out)
	}()

	return out, nil
}

func (s *multiplier) Close() error {
	var err error

	for _, sub := range s.subscribers {
		if closeErr := sub.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}

	return err
}
