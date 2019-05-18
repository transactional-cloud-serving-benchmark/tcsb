package serialization_util

import (
	"io"

	"github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

func DecodeNextReply(r io.Reader, bufp *[]byte) (reply serialized_messages.Reply, err error) {
	if bufp == nil {
		x := make([]byte, 0, 4)
		bufp = &x
	}
	if cap(*bufp) < 4 {
		*bufp = append(*bufp, make([]byte, 4-cap(*bufp))...)
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

	// Grow buf if needed.
	if cap(*bufp) < int(payloadLen) {
		needed := int(payloadLen) - cap(*bufp)
		*bufp = (*bufp)[:cap(*bufp)]
		*bufp = append(*bufp, make([]byte, needed)...)
	}

	// Resize bufp to the size of the payload.
	*bufp = (*bufp)[:int(payloadLen)]

	// Read the buf data.
	_, err = io.ReadFull(r, *bufp)
	if err != nil {
		return
	}

	reply = *serialized_messages.GetRootAsReply(*bufp, 0)

	return
}
