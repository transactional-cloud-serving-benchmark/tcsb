// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package serialized_messages

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type BatchWriteRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsBatchWriteRequest(buf []byte, offset flatbuffers.UOffsetT) *BatchWriteRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &BatchWriteRequest{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *BatchWriteRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *BatchWriteRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *BatchWriteRequest) KeyValuePairs(obj *KeyValuePair, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *BatchWriteRequest) KeyValuePairsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func BatchWriteRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func BatchWriteRequestAddKeyValuePairs(builder *flatbuffers.Builder, keyValuePairs flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(keyValuePairs), 0)
}
func BatchWriteRequestStartKeyValuePairsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func BatchWriteRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}