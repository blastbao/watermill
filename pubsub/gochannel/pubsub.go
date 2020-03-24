package gochannel

import (
	"context"
	"errors"
	"sync"

	"github.com/blastbao/watermill"
	"github.com/blastbao/watermill/message"
	"github.com/lithammer/shortuuid"
)

type Config struct {

	// Output channel buffer size.
	// Output channel buffer size.
	OutputChannelBuffer int64

	// If persistent is set to true, when subscriber subscribes to the topic,
	// it will receive all previously produced messages.
	//
	// All messages are persisted to the memory (simple slice),
	// so be aware that with large amount of messages you can go out of the memory.
	Persistent bool

	// When true, Publish will block until subscriber Ack's the message.
	// If there are no subscribers, Publish will not block (also when Persistent is true).
	BlockPublishUntilSubscriberAck bool
}

// GoChannel is the simplest Pub/Sub implementation.
// It is based on Golang's channels which are sent within the process.
//
// GoChannel has no global state,
// that means that you need to use the same instance for Publishing and Subscribing!
//
// When GoChannel is persistent, messages order is not guaranteed.
type GoChannel struct {
	config Config
	logger watermill.LoggerAdapter

	subscribersWg          sync.WaitGroup
	subscribers            map[string][]*subscriber
	subscribersLock        sync.RWMutex
	subscribersByTopicLock sync.Map // map of *sync.Mutex,  map[topic] => mutex

	closed     bool
	closedLock sync.Mutex
	closing    chan struct{}

	persistedMessages     map[string][]*message.Message
	persistedMessagesLock sync.RWMutex
}

// NewGoChannel creates new GoChannel Pub/Sub.
//
// This GoChannel is not persistent.
// That means if you send a message to a topic to which no subscriber is subscribed, that message will be discarded.
func NewGoChannel(config Config, logger watermill.LoggerAdapter) *GoChannel {

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &GoChannel{

		config: config,

		subscribers:            make(map[string][]*subscriber),
		subscribersByTopicLock: sync.Map{},

		logger: logger.With(watermill.LogFields{
			"pubsub_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),

		persistedMessages: map[string][]*message.Message{},
	}
}

// Publish in GoChannel is NOT blocking until all consumers consume.
// Messages will be send in background.
//
// Messages may be persisted or not, depending of persistent attribute.
func (g *GoChannel) Publish(topic string, messages ...*message.Message) error {

	// check if g has been closed
	if g.isClosed() {
		return errors.New("Pub/Sub closed")
	}

	// deep copy messages, preventing objects are changed by caller which may lead to unpredictable consequences.
	for i, msg := range messages {
		messages[i] = msg.Copy()
	}

	// get subscriber lock
	g.subscribersLock.RLock()
	defer g.subscribersLock.RUnlock()

	// get topic lock
	subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
	subLock.(*sync.Mutex).Lock()
	defer subLock.(*sync.Mutex).Unlock()

	// persist messages in memory queue, persistedMessages[topic] => [messages...]
	if g.config.Persistent {
		g.persistedMessagesLock.Lock()
		if _, ok := g.persistedMessages[topic]; !ok {
			g.persistedMessages[topic] = make([]*message.Message, 0)
		}
		g.persistedMessages[topic] = append(g.persistedMessages[topic], messages...)
		g.persistedMessagesLock.Unlock()
	}

	// send each message to all subscribers of topic
	for i := range messages {

		msg := messages[i]

		// start a backend goroutine sending msg to each subscriber
		ackedBySubscribers, err := g.sendMessage(topic, msg)
		if err != nil {
			return err
		}

		// if `BlockPublishUntilSubscriberAck` is true, blocking until all subscriber have received msg.
		if g.config.BlockPublishUntilSubscriberAck {
			g.waitForAckFromSubscribers(msg, ackedBySubscribers)
		}

	}

	return nil
}

func (g *GoChannel) waitForAckFromSubscribers(msg *message.Message, ackedByConsumer <-chan struct{}) {

	logFields := watermill.LogFields{"message_uuid": msg.UUID}
	g.logger.Debug("Waiting for subscribers ack", logFields)

	select {
	// waiting for the close signal from channel `ackedByConsumer`,
	// which indicates that the message has been send to each subscriber.
	case <-ackedByConsumer:
		g.logger.Trace("Message acked by subscribers", logFields)

	case <-g.closing:
		g.logger.Trace("Closing Pub/Sub before ack from subscribers", logFields)
	}
}

// start a backend goroutine sending msg to all subscribers of topic,
// return a channel which will be closed when push are finished, so caller could waiting for
// the close signal of channel to know when the send are finished.

func (g *GoChannel) sendMessage(topic string, message *message.Message) (<-chan struct{}, error) {

	logFields := watermill.LogFields{"message_uuid": message.UUID, "topic": topic}

	// get all subscribers of topic
	subscribers := g.topicSubscribers(topic)

	// channel 'ackedBySubscribers' will be closed when msg have been send to every subscriber.
	ackedBySubscribers := make(chan struct{})

	// check if don't need send
	if len(subscribers) == 0 {
		close(ackedBySubscribers)
		g.logger.Info("No subscribers to send message", logFields)
		return ackedBySubscribers, nil
	}

	// start a backend goroutine sending msg to each subscriber
	go func(subscribers []*subscriber) {

		for i := range subscribers {
			subscriber := subscribers[i]
			subscriber.sendMessageToSubscriber(message, logFields)
		}

		// when sending finished, close the 'ackedBySubscribers' channel to notify caller
		close(ackedBySubscribers)

	}(subscribers)

	return ackedBySubscribers, nil
}

// Subscribe returns channel to which all published messages are sent.
// Messages are not persisted. If there are no subscribers and message is produced it will be gone.
//
// There are no consumer groups support etc. Every consumer will receive every produced message.
func (g *GoChannel) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {

	// check if g is closed
	g.closedLock.Lock()
	if g.closed {
		return nil, errors.New("Pub/Sub closed")
	}
	g.subscribersWg.Add(1)
	g.closedLock.Unlock()

	// get subscriber lock
	g.subscribersLock.Lock()

	// get topic lock
	subLock, _ := g.subscribersByTopicLock.LoadOrStore(topic, &sync.Mutex{})
	subLock.(*sync.Mutex).Lock()


	// new a subscriber
	s := &subscriber{
		ctx:           ctx,
		uuid:          watermill.NewUUID(),
		outputChannel: make(chan *message.Message, g.config.OutputChannelBuffer),
		logger:        g.logger,
		closing:       make(chan struct{}),
	}


	go func(s *subscriber, g *GoChannel) {


		// 1. blocking until g is closed or ctx is time out.
		select {
		case <-ctx.Done():
			// unblock
		case <-g.closing:
			// unblock
		}

		// 2. close s
		s.Close()


		// get subscriber lock
		g.subscribersLock.Lock()
		defer g.subscribersLock.Unlock()

		// get topic lock
		subLock, _ := g.subscribersByTopicLock.Load(topic)
		subLock.(*sync.Mutex).Lock()
		defer subLock.(*sync.Mutex).Unlock()

		// 4. remove sub from g.subscribers[topic]
		g.removeSubscriber(topic, s)

		g.subscribersWg.Done()

	}(s, g)


	// if g.config.Persistent == false, call 'g.addSubscriber(topic, s)' add s to g.subscribers[topic] then return
	if !g.config.Persistent {
		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()
		g.addSubscriber(topic, s) // add s to g.subscribers[topic]
		return s.outputChannel, nil
	}

	// else, g.config.Persistent == true, get the cached msg and send them to s, then call 'g.addSubscriber(topic, s)'
	go func(s *subscriber) {

		defer g.subscribersLock.Unlock()
		defer subLock.(*sync.Mutex).Unlock()

		g.persistedMessagesLock.RLock()
		messages, ok := g.persistedMessages[topic]
		g.persistedMessagesLock.RUnlock()

		if ok {
			for i := range messages {
				msg := g.persistedMessages[topic][i]
				logFields := watermill.LogFields{"message_uuid": msg.UUID, "topic": topic}

				go s.sendMessageToSubscriber(msg, logFields)
			}
		}

		g.addSubscriber(topic, s)
	}(s)

	// attention, return the private member field 'outputChannel' of s to the caller.
	return s.outputChannel, nil
}

// add sub to g.subscribers[topic]
func (g *GoChannel) addSubscriber(topic string, s *subscriber) {
	if _, ok := g.subscribers[topic]; !ok {
		g.subscribers[topic] = make([]*subscriber, 0)
	}
	g.subscribers[topic] = append(g.subscribers[topic], s)
}

// remove sub from g.subscribers[topic]
func (g *GoChannel) removeSubscriber(topic string, toRemove *subscriber) {
	removed := false
	for i, sub := range g.subscribers[topic] {
		if sub == toRemove {
			g.subscribers[topic] = append(g.subscribers[topic][:i], g.subscribers[topic][i+1:]...)
			removed = true
			break
		}
	}
	if !removed {
		panic("cannot remove subscriber, not found " + toRemove.uuid)
	}
}

// get subs of topic
func (g *GoChannel) topicSubscribers(topic string) []*subscriber {
	subscribers, ok := g.subscribers[topic]
	if !ok {
		return nil
	}

	return subscribers
}

func (g *GoChannel) isClosed() bool {
	g.closedLock.Lock()
	defer g.closedLock.Unlock()

	return g.closed
}

func (g *GoChannel) Close() error {
	g.closedLock.Lock()
	defer g.closedLock.Unlock()

	// already closed
	if g.closed {
		return nil
	}

	// set close flag to true
	g.closed = true

	// close notify channel
	close(g.closing)

	g.logger.Debug("Closing Pub/Sub, waiting for subscribers", nil)

	// wait for all backend goroutine end worker
	g.subscribersWg.Wait()

	g.logger.Info("Pub/Sub closed", nil)

	// clear memory-based cache
	g.persistedMessages = nil

	return nil
}

type subscriber struct {
	ctx context.Context

	uuid string

	sending       sync.Mutex
	outputChannel chan *message.Message

	logger  watermill.LoggerAdapter
	closed  bool
	closing chan struct{}
}

func (s *subscriber) Close() {
	if s.closed {
		return
	}
	close(s.closing)

	s.logger.Debug("Closing subscriber, waiting for sending lock", nil)

	// ensuring that we are not sending to closed channel
	s.sending.Lock()
	defer s.sending.Unlock()

	s.logger.Debug("GoChannel Pub/Sub Subscriber closed", nil)
	s.closed = true

	close(s.outputChannel)
}

//
func (s *subscriber) sendMessageToSubscriber(msg *message.Message, logFields watermill.LogFields) {

	s.sending.Lock()
	defer s.sending.Unlock()

	ctx, cancelCtx := context.WithCancel(s.ctx)
	defer cancelCtx()

SendToSubscriber:

	for {

		// copy the message to prevent ack/nack propagation to other consumers
		// also allows to make retries on a fresh copy of the original message
		msgToSend := msg.Copy()
		msgToSend.SetContext(ctx)

		s.logger.Trace("Sending msg to subscriber", logFields)

		// check if closed
		if s.closed {
			s.logger.Info("Pub/Sub closed, discarding msg", logFields)
			return
		}

		// send msg to outChan, creator of this subscriber will receive the msg, handle it, then send ACK/NACK.
		select {
		case s.outputChannel <- msgToSend:
			s.logger.Trace("Sent message to subscriber", logFields)
		case <-s.closing:
			s.logger.Trace("Closing, message discarded", logFields)
			return
		}

		// waiting for ack
		select {
		// if msg is acked, return
		case <-msgToSend.Acked():
			s.logger.Trace("Message acked", logFields)
			return
		// if msg is nacked, need retry sending
		case <-msgToSend.Nacked():
			s.logger.Trace("Nack received, resending message", logFields)
			continue SendToSubscriber
		case <-s.closing:
			s.logger.Trace("Closing, message discarded", logFields)
			return
		}
	}
}
