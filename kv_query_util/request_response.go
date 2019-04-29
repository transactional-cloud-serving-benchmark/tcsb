package kv_query_util

import (
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type RequestResponse struct {
	backingBuffer []byte

	batchCmd serialized_messages.BatchKeyValueCommand
	op       serialized_messages.KeyValueCommandOp

	completed bool

	readkey, readval []byte // only used when readOps > 0

	readOps, writeOps uint64
}

func NewRequestResponse() RequestResponse {
	rr := RequestResponse{
		backingBuffer: []byte{},
		readkey:       []byte{},
		readval:       []byte{},
	}
	rr.Reset()
	return rr
}

func (x *RequestResponse) Reset() {
	x.backingBuffer = x.backingBuffer[:0]

	x.batchCmd = serialized_messages.BatchKeyValueCommand{}
	x.op = -1

	x.completed = false

	x.readkey = x.readkey[:0]
	x.readval = x.readval[:0]

	x.readOps = 0
	x.writeOps = 0
}
