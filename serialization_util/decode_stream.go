package serialization_util

import (
	"fmt"
	"io"

	"github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type MessageReader struct { }

func NewMessageReader() *MessageReader { return &MessageReader{} }

func (mr *MessageReader) Next(r io.Reader, bufp *[]byte) (batchCmd serialized_messages.BatchKeyValueCommand, op serialized_messages.KeyValueCommandOp, err error) {
	if bufp == nil {
		x := make([]byte, 0, 4)
		bufp = &x
	}
	if cap(*bufp) < 4 {
		*bufp = append(*bufp, make([]byte, 4 - cap(*bufp))...)
	}

	// Read framing format: little-endian uint32.
	*bufp = (*bufp)[:4]

	_, err = io.ReadFull(r, *bufp)
	// Applies also to err == io.EOF
	if err != nil {
		return
	}

	// Decode payload length from framing format.
	payloadLen := flatbuffers.GetUint32(*bufp)

	*bufp = (*bufp)[:cap(*bufp)]

	// Grow buf if needed.
	for len(*bufp) < int(payloadLen) {
		*bufp = append(*bufp, make([]byte, len(*bufp))...)
	}

	// Resize bufp to the size of the payload.
	*bufp = (*bufp)[:int(payloadLen)]

	// Read the buf data.
	_, err = io.ReadFull(r, *bufp)
	if err != nil {
		return
	}

	// Access the payload data
	batchCmd = *serialized_messages.GetRootAsBatchKeyValueCommand(*bufp, 0)
	if batchCmd.CommandsLength() == 0 {
		err = fmt.Errorf("bad data: empty batch commands")
		return
	}
	scratchCmd := serialized_messages.KeyValueCommand{}
	batchCmd.Commands(&scratchCmd, 0)
	op = scratchCmd.Op() // by construction, the batch op types will all be the same.
	return
}
