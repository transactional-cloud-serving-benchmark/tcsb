package simulation

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type KeyValueScenario struct {
	DatabaseKind string
	CommandMode  string
	EmitMode     string

	Seed         int64
	WriteRatio   float64
	CommandCount int64
	KeyOrdering  string

	WriteBatchSize int
	KeyLen, ValLen int
}

func (s *KeyValueScenario) SetDatabaseKindByName(x string) error {
	switch x {
	case "gold_memory":
		s.DatabaseKind = x
	default:
		return fmt.Errorf("unknown database kind '%s'", x)
	}
	return nil
}

func (s *KeyValueScenario) SetCommandModeByName(x string) error {
	switch x {
	case "exec":
		s.CommandMode = x
	default:
		return fmt.Errorf("unknown command mode '%s'", x)
	}
	return nil
}

func (s *KeyValueScenario) SetEmitModeByName(x string) error {
	switch x {
	case "debug", "binary":
		s.EmitMode = x
	default:
		return fmt.Errorf("unknown emit mode '%s'", x)
	}
	return nil
}

func (s *KeyValueScenario) SetParamsFromString(x string) error {
	m := map[string]string{}
	fields := strings.Split(x, ",")
	for _, field := range fields {
		subfields := strings.Split(field, "=")
		if len(subfields) != 2 {
			return fmt.Errorf("unexpected param formatting")
		}
		key := subfields[0]
		val := subfields[1]
		m[key] = val
	}

	req_params := []string{"seed", "write_ratio", "command_count", "key_ordering", "key_len", "val_len", "write_batch_size"}
	for _, k := range req_params {
		if _, ok := m[k]; !ok {
			return fmt.Errorf("required param: %s", k)
		}
	}
outer:
	for k := range m {
		for _, k2 := range req_params {
			if k == k2 {
				continue outer
			}
		}
		return fmt.Errorf("unknown param: %s", k)
	}

	if n, err := strconv.Atoi(m["seed"]); err == nil {
		s.Seed = int64(n)
	} else {
		return err
	}

	if n, err := strconv.ParseFloat(m["write_ratio"], 64); err == nil {
		if n <= 0. || 1. < n {
			return fmt.Errorf("write_ratio must be in the range (0, 1]")
		}
		s.WriteRatio = n
	} else {
		return err
	}
	if n, err := strconv.Atoi(m["command_count"]); err == nil {
		s.CommandCount = int64(n)
	}

	switch m["key_ordering"] {
	case "sorted", "random":
		s.KeyOrdering = m["key_ordering"]
	default:
		return fmt.Errorf("unknown key_ordering, choose from random or sorted")
	}

	if n, err := strconv.Atoi(m["write_batch_size"]); err == nil {
		s.WriteBatchSize = int(n)
	} else {
		return err
	}

	if n, err := strconv.Atoi(m["key_len"]); err == nil {
		s.KeyLen = int(n)
	} else {
		return err
	}

	if n, err := strconv.Atoi(m["val_len"]); err == nil {
		s.ValLen = int(n)
	} else {
		return err
	}
	return nil
}

func (s *KeyValueScenario) NewEmitter(w io.Writer) Emitter {
	var batchIdx int64 = 0
	var commandsGenerated int64 = 0
	var numWritten int64 = 0

	var builder *flatbuffers.Builder

	keysBacking := make([]byte, s.KeyLen*s.WriteBatchSize)
	valsBacking := make([]byte, s.KeyLen*s.WriteBatchSize)
	keys := make([][]byte, 0, s.KeyLen)
	vals := make([][]byte, 0, s.ValLen)

	setKVBacking := func(n int) {
		keys = keys[:0]
		vals = vals[:0]

		for i := 0; i < n; i++ {
			keys = append(keys, keysBacking[i*s.KeyLen:(i+1)*s.KeyLen])
			vals = append(vals, valsBacking[i*s.ValLen:(i+1)*s.ValLen])
		}
	}

	var ops KeyValueOpGen
	if s.KeyOrdering == "random" {
		ops = NewRandomOpGen(s.Seed)
	} else {
		ops = NewOrderedOpGen(s.Seed)
	}

	return func() error {
		if commandsGenerated >= s.CommandCount {
			return io.EOF
		}

		actualRatio := float64(numWritten) / float64(commandsGenerated)
		opRead := numWritten > 0 && actualRatio > s.WriteRatio

		if opRead {
			setKVBacking(1)
			ops.NextReadOp(keys[0])
			commandsGenerated++
		} else {
			setKVBacking(s.WriteBatchSize)
			for idx, _ := range keys {
				ops.NextWriteOp(keys[idx], vals[idx])
				numWritten++
				commandsGenerated++
			}
		}

		if s.EmitMode == "debug" {
			emitDebugKeyValue(commandsGenerated, batchIdx, w, opRead, keys, vals)
		} else {
			if builder == nil {
				builder = flatbuffers.NewBuilder(1024)
			}

			emitFlatBuffersKeyValue(builder, batchIdx, w, opRead, keys, vals)
		}

		keys = keys[:0]
		vals = vals[:0]

		batchIdx++

		return nil
	}
}

func emitDebugKeyValue(commandsGenerated, batchIdx int64, w io.Writer, opRead bool, keys, vals [][]byte) {
	startingId := int(commandsGenerated) - len(keys)
	for j, _ := range keys {
		if opRead {
			fmt.Fprintf(w, "%d %d %d r %s\n", startingId+j, batchIdx, j, keys[j])
		} else {
			fmt.Fprintf(w, "%d %d %d w %s %s\n", startingId+j, batchIdx, j, keys[j], vals[j])
		}
	}
}

func emitFlatBuffersKeyValue(builder *flatbuffers.Builder, i int64, w io.Writer, opRead bool, keys, vals [][]byte) {
	builder.Reset()

	if opRead {
		if len(keys) != 1 {
			log.Fatal("bad assumption in scenario generation")
		}
		keyOffset := builder.CreateByteVector(keys[0])

		serialized_messages.ReadRequestStart(builder)
		serialized_messages.ReadRequestAddKey(builder, keyOffset)
		rrOffset := serialized_messages.ReadRequestEnd(builder)

		serialized_messages.RequestStart(builder)
		serialized_messages.RequestAddRequestUnionType(builder, byte(serialized_messages.RequestUnionReadRequest))
		serialized_messages.RequestAddRequestUnion(builder, rrOffset)
		reqOffset := serialized_messages.RequestEnd(builder)

		builder.Finish(reqOffset)

	} else {
		kvpOffsets := make([]flatbuffers.UOffsetT, len(keys))

		for idx, _ := range keys {
			keyOffset := builder.CreateByteVector(keys[idx])
			valOffset := builder.CreateByteVector(vals[idx])

			serialized_messages.KeyValuePairStart(builder)
			serialized_messages.KeyValuePairAddKey(builder, keyOffset)
			serialized_messages.KeyValuePairAddValue(builder, valOffset)
			kvpOffsets[idx] = serialized_messages.KeyValuePairEnd(builder)
		}

		serialized_messages.BatchWriteRequestStartKeyValuePairsVector(builder, len(kvpOffsets))
		for j := len(kvpOffsets) - 1; j >= 0; j-- {
			builder.PrependUOffsetT(kvpOffsets[j])
		}
		kvpVectorOffset := builder.EndVector(len(kvpOffsets))

		serialized_messages.BatchWriteRequestStart(builder)
		serialized_messages.BatchWriteRequestAddKeyValuePairs(builder, kvpVectorOffset)
		bwrOffset := serialized_messages.BatchWriteRequestEnd(builder)

		serialized_messages.RequestStart(builder)
		serialized_messages.RequestAddRequestUnionType(builder, byte(serialized_messages.RequestUnionBatchWriteRequest))
		serialized_messages.RequestAddRequestUnion(builder, bwrOffset)
		reqOffset := serialized_messages.RequestEnd(builder)

		builder.Finish(reqOffset)
	}

	// Framing format: length in bytes as a uint32.
	builder.PrependUint32(uint32(len(builder.FinishedBytes())))

	w.Write(builder.FinishedBytes())
}

type KeyValueOpGen interface {
	NextReadOp([]byte)
	NextWriteOp([]byte, []byte)
}

// RandomOpGen wraps logic to track state, and generate read and write
// requests, for random key/value ops.
type RandomOpGen struct {
	OpRng       *rand.Rand
	DataGenSeed int64
	NWritten    int64
	NTotal      int64
}

func NewRandomOpGen(opSeed int64) *RandomOpGen {
	opRng := rand.New(rand.NewSource(opSeed))
	dataGenSeed := opRng.Int63()

	return &RandomOpGen{
		OpRng:       opRng,
		DataGenSeed: dataGenSeed,
		NWritten:    0,
		NTotal:      0,
	}
}

func (rog *RandomOpGen) NextReadOp(dstkey []byte) {
	if rog.NWritten <= 0 {
		panic("logic error: cannot call NextReadOp until NextWriteOp has been called at least once")
	}
	// Choose a seed from the set of previously-written sequence numbers.
	seqNo := rog.OpRng.Int63n(rog.NWritten)

	// Use the seed to regenerate the key byte slice. Guaranteed to have
	// been used in a write op beforehand.
	r := rand.New(rand.NewSource(rog.DataGenSeed + seqNo + 1))
	randAscii(r, dstkey)

	rog.NTotal++
}

func (rog *RandomOpGen) NextWriteOp(dstkey, dstval []byte) {
	// Choose a seed from the set of previously-written sequence numbers.
	seqNo := rog.NWritten

	//// Use the sequence number to generate the key byte slice.
	//seqAscii(uint64(seqNo), dstkey)

	// Use the seed and the sequance number to generate the key and value byte slices.
	r := rand.New(rand.NewSource(rog.DataGenSeed + seqNo + 1))
	randAscii(r, dstkey)
	randAscii(r, dstval)

	rog.NWritten++
	rog.NTotal++
}

// OrderedOpGen wraps logic to track state, and generate read and write
// requests, for ordered write ops and random read  ops.
type OrderedOpGen struct {
	OpRng       *rand.Rand
	DataGenSeed int64
	NWritten    int64
	NTotal      int64
}

func NewOrderedOpGen(opSeed int64) *OrderedOpGen {
	opRng := rand.New(rand.NewSource(opSeed))
	dataGenSeed := opRng.Int63()

	return &OrderedOpGen{
		OpRng:       opRng,
		DataGenSeed: dataGenSeed,
		NWritten:    0,
		NTotal:      0,
	}
}

func (oog *OrderedOpGen) NextReadOp(dstkey []byte) {
	if oog.NWritten <= 0 {
		panic("logic error: cannot call NextReadOp until NextWriteOp has been called at least once")
	}
	// Choose a seed from the set of previously-written sequence numbers.
	seqNo := oog.OpRng.Int63n(oog.NWritten)

	// Use the sequence number to regenerate the key byte slice.
	// Guaranteed to have been used in a write op beforehand.
	seqAscii(uint64(seqNo), dstkey)

	oog.NTotal++
}

func (oog *OrderedOpGen) NextWriteOp(dstkey, dstval []byte) {
	seqNo := oog.NWritten

	// Use the sequence number to generate the key byte slice.
	seqAscii(uint64(seqNo), dstkey)

	// Use the seed and the sequance number to generate the value byte slice.
	r := rand.New(rand.NewSource(oog.DataGenSeed + seqNo + 1))
	randAscii(r, dstval)

	oog.NWritten++
	oog.NTotal++
}

func randAscii(r *rand.Rand, b []byte) {
	const choices = "abcdefghijklmnopqrstuvwxyz"
	for i := range b {
		b[i] = choices[r.Intn(len(choices))]
	}
}

func seqAscii(id uint64, b []byte) {
	const choices = "abcdefghijklmnopqrstuvwxyz"
	for i := range b {
		m := id % uint64(len(choices))
		b[len(b)-i-1] = choices[int(m)]
		id = (id - m) / uint64(len(choices))
	}
}
