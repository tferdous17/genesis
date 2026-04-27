package store

type MinRecordHeap []*Record

func (h MinRecordHeap) Len() int {
	return len(h)
}

func (h MinRecordHeap) Less(i, j int) bool {
	return h[i].Key < h[j].Key
}

func (h MinRecordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinRecordHeap) Push(val interface{}) {
	*h = append(*h, val.(*Record))
}

func (h *MinRecordHeap) Pop() interface{} {
	old := *h
	size := len(old)
	val := old[size-1]
	old[size-1] = nil // nil out the pointer to allow garbage collection
	*h = old[:size-1]
	return val
}
