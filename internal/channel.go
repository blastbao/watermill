package internal

// IsChannelClosed returns true if provided `chan struct{}` is closed.
// IsChannelClosed panics if message is sent to this channel.
func IsChannelClosed(channel chan struct{}) bool {
	select {

	// if channel is closed, always meet this case and flag 'ok' is false.
	case _, ok := <-channel:

		// if ok is true, means the channel is not closed and has recently received a msg from it.
		if ok {
			panic("received unexpected message")
		}

		return true

	// if channel is blocking, then meet case default
	default:
		return false
	}
}
