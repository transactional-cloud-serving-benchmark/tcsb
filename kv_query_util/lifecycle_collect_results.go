package kv_query_util

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/google/flatbuffers/go"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type ReplyCollector struct {
	start time.Time

	printInterval time.Duration
	nextPrintAt   time.Time

	nBurnIn, nValidation uint64

	doingBurnIn     bool
	doingValidation bool

	validationFile   *os.File
	validationWriter *bufio.Writer

	validations, burnins        uint64
	readOps, writeOps           uint64
	readRequests, writeRequests uint64
}

func NewReplyCollector(nBurnIn, nValidation uint64, validationFilename string) *ReplyCollector {
	printInterval := time.Second * 30
	rc := &ReplyCollector{
		nBurnIn:     nBurnIn,
		nValidation: nValidation,

		printInterval: printInterval,
		nextPrintAt:   time.Now().Add(printInterval),

		start: time.Now(),

		doingBurnIn:     nBurnIn > 0,
		doingValidation: nValidation > 0,
	}

	if rc.doingValidation {
		var err error
		rc.validationFile, err = os.Create(validationFilename)
		if err != nil {
			log.Fatal(err)
		}

		rc.validationWriter = bufio.NewWriter(rc.validationFile)
	}

	return rc
}

func (rc *ReplyCollector) Update(reply serialized_messages.Reply) {
	if reply.ReplyUnionType() == serialized_messages.ReplyUnionReadReply {
		// Decode the ReadReply.
		t := flatbuffers.Table{}
		if ok := reply.ReplyUnion(&t); !ok {
			log.Fatal("logic error: bad ReplyUnion decoding")
		}

		rr := serialized_messages.ReadReply{}
		rr.Init(t.Bytes, t.Pos)

		// If burn-in is occurring, check if it needs to be stopped. If
		// so, reset the timer and stop burn-in.
		if rc.doingBurnIn {
			rc.burnins += 1
			if rc.burnins >= rc.nBurnIn {
				rc.doingBurnIn = false
				// Reset statistics timer when burn-in is complete:
				rc.start = time.Now()
			}
		} else {
			rc.readOps++
			rc.readRequests++
		}

		// If validation is occurring, print the result, then check if
		// validation needs to be stopped.
		if rc.doingValidation {
			rc.validations++
			fmt.Fprintf(rc.validationWriter, "%s -> %s\n", rr.KeyBytes(), rr.ValueBytes())
			if rc.validations >= rc.nValidation {
				rc.doingValidation = false
				rc.validationWriter.Flush()
				rc.validationFile.Close()
			}
		}
	} else if reply.ReplyUnionType() == serialized_messages.ReplyUnionBatchWriteReply {
		// Decode the BatchWriteReply.
		t := flatbuffers.Table{}
		if ok := reply.ReplyUnion(&t); !ok {
			log.Fatal("logic error: bad ReplyUnion decoding")
		}

		rr := serialized_messages.BatchWriteReply{}
		rr.Init(t.Bytes, t.Pos)

		// If burn-in is occurring, check if it needs to be stopped. If
		// so, reset the timer and stop burn-in.
		if rc.doingBurnIn {
			rc.burnins += 1
			if rc.burnins >= rc.nBurnIn {
				rc.doingBurnIn = false
				// Reset statistics timer when burn-in is complete:
				rc.start = time.Now()
			}
		} else {
			rc.writeOps += rr.NWrites()
			rc.writeRequests++
		}
	} else {
		log.Fatal("unknown ReplyUnionType")
	}

	if time.Now().After(rc.nextPrintAt) {
		log.Printf("benchmark running for %d seconds:\n", int(math.Round(time.Since(rc.start).Seconds())))
		rc.printStats()
		fmt.Println("")
		rc.nextPrintAt = rc.nextPrintAt.Add(rc.printInterval)
	}
}
func (rc *ReplyCollector) Finish() {
	log.Printf("benchmark complete:\n")
	rc.printStats()
}

func (rc *ReplyCollector) printStats() {
	end := time.Now()
	tookNanos := float64(end.Sub(rc.start).Nanoseconds())
	secs := tookNanos / 1e9

	fmt.Printf("  %d read operations logged for validation\n", rc.validations)
	fmt.Printf("  %d operations executed before beginning statistics collection (burn-in)\n", rc.burnins)
	fmt.Printf("  %d read batch requests\n", rc.readRequests)
	fmt.Printf("  %d write batch requests\n", rc.writeRequests)
	fmt.Printf("  %d read operations\n", rc.readOps)
	fmt.Printf("  %d write operations\n", rc.writeOps)
	fmt.Printf("  %d total operations\n", rc.readOps+rc.writeOps)
	fmt.Printf("  %.1f write ops/sec\n", float64(rc.writeOps)/secs)
	fmt.Printf("  %.1f read ops/sec\n", float64(rc.readOps)/secs)
	// TODO(rw): ensure this is sensible fmt.Printf("  %d ns/read op\n", int64(tookNanos/float64(rc.readOps)))
	// TODO(rw): ensure this is sensible fmt.Printf("  %d ns/write op\n", int64(tookNanos/float64(rc.writeOps)))
}
