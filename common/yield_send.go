package common

// NonBlockingSafeSend implements a channel send that is guaranteed to not panic, and may be either instantaneous or waiting.
// Instantaneous returns false if a panic occurred, or if
func NonBlockingSafeSend[T any](target chan<- T, value T, instant ...bool) <-chan bool {
	out := make(chan bool, 1)

	sendInstant := func() bool {
		select {
		case target <- value:
			return true
		default:
			return false
		}
	}

	sendWaiting := func() bool {
		target <- value
		return true
	}

	go func() {
		defer func() { // Catch in case this send causes issues.
			if err := recover(); err != nil {
				out <- false
				close(out)
			}
		}()

		// Then try to send down the channel
		result := Iff(FirstOrZero(instant), sendInstant, sendWaiting)()
		out <- result
		close(out)
	}()

	return out
}
