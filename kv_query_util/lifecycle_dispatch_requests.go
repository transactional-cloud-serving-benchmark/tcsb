package kv_query_util

import (
	"bufio"
	"io"
	"log"
	"os"
	"sync"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
)

func DispatchRequestsToWorkers(rrPool *sync.Pool, jobs chan RequestResponse) {
	// For each binary payload on stdin, decode it and queue it for sending
	// to the server.
	in := bufio.NewReader(os.Stdin)
	msgReader := serialization_util.NewMessageReader()
	for {
		rr := rrPool.Get().(RequestResponse)
		batchCmd, op, err := msgReader.Next(in, &rr.backingBuffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		rr.batchCmd = batchCmd
		rr.op = op

		jobs <- rr
	}
}
