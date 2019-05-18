package serialization_util

import (
	"github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

func EncodeReadReplyWithFraming(builder *flatbuffers.Builder, key, val []byte) {
	builder.Reset()

	// Populate the reply key and value vectors:
	keyOffset := builder.CreateByteVector(key)
	valOffset := builder.CreateByteVector(val)

	// Populate the ReadReply FlatBuffers table:
	serialized_messages.ReadReplyStart(builder)
	serialized_messages.ReadReplyAddKey(builder, keyOffset)
	serialized_messages.ReadReplyAddValue(builder, valOffset)
	replyOffset := serialized_messages.ReadReplyEnd(builder)

	// Populate the Reply FlatBuffers table:
	serialized_messages.ReplyStart(builder)
	serialized_messages.ReplyAddReplyUnionType(builder, byte(serialized_messages.ReplyUnionReadReply))
	serialized_messages.ReplyAddReplyUnion(builder, replyOffset)
	end := serialized_messages.ReplyEnd(builder)

	// Finish the FlatBuffer:
	builder.Finish(end)

	// Write the framing format:
	builder.PrependUint32(uint32(len(builder.FinishedBytes())))
}

func EncodeBatchWriteReplyWithFraming(builder *flatbuffers.Builder, nWritten uint64) {
	builder.Reset()

	// Populate the BatchWriteReply FlatBuffers table:
	serialized_messages.BatchWriteReplyStart(builder)
	serialized_messages.BatchWriteReplyAddNWrites(builder, nWritten)
	replyOffset := serialized_messages.BatchWriteReplyEnd(builder)

	// Populate the Reply FlatBuffers table:
	serialized_messages.ReplyStart(builder)
	serialized_messages.ReplyAddReplyUnionType(builder, byte(serialized_messages.ReplyUnionBatchWriteReply))
	serialized_messages.ReplyAddReplyUnion(builder, replyOffset)
	end := serialized_messages.ReplyEnd(builder)

	// Finish the FlatBuffer:
	builder.Finish(end)

	// Write the framing format:
	builder.PrependUint32(uint32(len(builder.FinishedBytes())))

}
