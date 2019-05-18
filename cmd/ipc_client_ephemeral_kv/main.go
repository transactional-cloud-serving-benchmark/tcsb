package main

import (
	"bufio"
	"log"
	"os"

	"github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

func main() {
	data := make(map[string]string, 1024)

	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	defer func() {
		out.Flush()
		os.Stdout.Close()
	}()

	buf := make([]byte, 0, 1024)
	buf = buf[:0]
	builder := flatbuffers.NewBuilder(1024)

	for {
		req, err := serialization_util.DecodeNextCommand(in, &buf)
		if err != nil {
			break
		}

		if req.RequestUnionType() == serialized_messages.RequestUnionReadRequest {
			// Decode read request
			t := flatbuffers.Table{}
			if ok := req.RequestUnion(&t); !ok {
				log.Fatal("logic error: bad RequestUnion decoding")
			}

			rr := serialized_messages.ReadRequest{}
			rr.Init(t.Bytes, t.Pos)

			if len(rr.KeyBytes()) == 0 {
				log.Fatal("missing keybytes")
			}

			// Get the value from the in-memory map.
			// Val will be the empty string if it was not found,
			// which is acceptable for validation purposes.
			val, _ := data[string(rr.KeyBytes())]

			{
				// Encode the read reply information for the IPC driver.
				serialization_util.EncodeReadReplyWithFraming(builder, rr.KeyBytes(), []byte(val))

				// Write to the output sink:
				out.Write(builder.FinishedBytes())
			}
		} else if req.RequestUnionType() == serialized_messages.RequestUnionBatchWriteRequest {
			// Decode batch write request:
			t := flatbuffers.Table{}
			if ok := req.RequestUnion(&t); !ok {
				log.Fatal("logic error: bad RequestUnion decoding")
			}

			bwr := serialized_messages.BatchWriteRequest{}
			bwr.Init(t.Bytes, t.Pos)

			// Populate the data writes:
			for i := 0; i < bwr.KeyValuePairsLength(); i++ {
				kvp := serialized_messages.KeyValuePair{}
				bwr.KeyValuePairs(&kvp, i)

				data[string(kvp.KeyBytes())] = string(kvp.ValueBytes())
			}

			{
				// Encode the batch write reply information for the IPC driver.
				serialization_util.EncodeBatchWriteReplyWithFraming(builder, uint64(bwr.KeyValuePairsLength()))

				// Write to the output sink:
				out.Write(builder.FinishedBytes())
			}
		} else {
			log.Fatal("logic error: invalid request type")
		}

		builder.Reset()
	}
}
