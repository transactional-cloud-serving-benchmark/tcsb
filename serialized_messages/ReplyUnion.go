// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package serialized_messages

import "strconv"

type ReplyUnion byte

const (
	ReplyUnionNONE            ReplyUnion = 0
	ReplyUnionReadReply       ReplyUnion = 1
	ReplyUnionBatchWriteReply ReplyUnion = 2
)

var EnumNamesReplyUnion = map[ReplyUnion]string{
	ReplyUnionNONE:            "NONE",
	ReplyUnionReadReply:       "ReadReply",
	ReplyUnionBatchWriteReply: "BatchWriteReply",
}

var EnumValuesReplyUnion = map[string]ReplyUnion{
	"NONE":            ReplyUnionNONE,
	"ReadReply":       ReplyUnionReadReply,
	"BatchWriteReply": ReplyUnionBatchWriteReply,
}

func (v ReplyUnion) String() string {
	if s, ok := EnumNamesReplyUnion[v]; ok {
		return s
	}
	return "ReplyUnion(" + strconv.FormatInt(int64(v), 10) + ")"
}