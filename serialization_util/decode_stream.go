package serialization_util

import (
	"fmt"
	"io"

	"github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type MessageReader struct {
	buf []byte
}

func NewMessageReader() *MessageReader {
	return &MessageReader{
		buf: make([]byte, 0, 1024),
	}
}

func (mr *MessageReader) Next(r io.Reader) (batchCmd serialized_messages.BatchKeyValueCommand, op serialized_messages.KeyValueCommandOp, err error) {
	buf := mr.buf

	// Read framing format: little-endian uint32.
	buf = buf[:4] // buf will always have room for this

	_, err = io.ReadFull(r, buf)
	// Applies also to err == io.EOF
	if err != nil {
		return
	}

	// Decode payload length from framing format.
	payloadLen := flatbuffers.GetUint32(buf)

	buf = buf[:cap(buf)]

	// Grow buf if needed.
	for len(buf) < int(payloadLen) {
		buf = append(buf, make([]byte, len(buf))...)
	}

	// Resize buf to the size of the payload.
	buf = buf[:int(payloadLen)]

	// Read the buf data.
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return
	}

	// Access the payload data
	batchCmd = *serialized_messages.GetRootAsBatchKeyValueCommand(buf, 0)
	if batchCmd.CommandsLength() == 0 {
		err = fmt.Errorf("bad data: empty batch commands")
		return
	}
	scratchCmd := serialized_messages.KeyValueCommand{}
	batchCmd.Commands(&scratchCmd, 0)
	op = scratchCmd.Op() // by construction, the batch op types will all be the same.
	return
}
