// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package serialized_messages

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type ReadRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsReadRequest(buf []byte, offset flatbuffers.UOffsetT) *ReadRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &ReadRequest{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *ReadRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *ReadRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *ReadRequest) Key(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *ReadRequest) KeyLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *ReadRequest) KeyBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *ReadRequest) MutateKey(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func ReadRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func ReadRequestAddKey(builder *flatbuffers.Builder, key flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(key), 0)
}
func ReadRequestStartKeyVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func ReadRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
