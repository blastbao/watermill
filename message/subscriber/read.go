package subscriber

import (
	"time"

	"github.com/blastbao/watermill/message"
)

// BulkRead reads provided amount of messages from the provided channel, until a timeout occurrs or the limit is reached.
func BulkRead(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages message.Messages, all bool) {

MessagesLoop:
	for len(receivedMessages) < limit {
		select {
		case msg, ok := <-messagesCh:

			// if channel is closed, break out and return
			if !ok {
				break MessagesLoop
			}

			// save msg in slice
			receivedMessages = append(receivedMessages, msg)

			// ack the msg
			msg.Ack()

		// if timeout, break and return
		case <-time.After(timeout):
			break MessagesLoop
		}
	}

	return receivedMessages, len(receivedMessages) == limit
}

// BulkReadWithDeduplication reads provided number of messages from the provided channel,
// ignoring duplicates, until a timeout occurrs or the limit is reached.
func BulkReadWithDeduplication(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages message.Messages, all bool) {

	receivedIDs := map[string]struct{}{}

MessagesLoop:
	for len(receivedMessages) < limit {
		select {
		case msg, ok := <-messagesCh:
			if !ok {
				break MessagesLoop
			}

			if _, ok := receivedIDs[msg.UUID]; !ok {
				receivedIDs[msg.UUID] = struct{}{}
				receivedMessages = append(receivedMessages, msg)
			}
			msg.Ack()
		case <-time.After(timeout):
			break MessagesLoop
		}
	}

	return receivedMessages, len(receivedMessages) == limit
}
