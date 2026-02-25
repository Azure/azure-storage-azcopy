package pacer

type requestQueueEntry struct {
	req     Request
	readyCh chan any
}
