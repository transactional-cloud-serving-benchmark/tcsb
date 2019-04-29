package kv_query_util

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

func CollectResponsesAndPrintResults(nBurnIn, nValidation uint64, validationFilename string, rrPool *sync.Pool, crrs chan RequestResponse) {
	start := time.Now()

	var doingBurnIn bool = nBurnIn > 0
	var doingValidation bool = nValidation > 0

	var validationFile *os.File
	var validationWriter *bufio.Writer

	if doingValidation {
		var err error
		validationFile, err = os.Create(validationFilename)
		if err != nil {
			log.Fatal(err)
		}

		validationWriter = bufio.NewWriter(validationFile)
	}

	var validations, burnins uint64
	var readOps, writeOps uint64
	var readRequests, writeRequests uint64

	for crr := range crrs {
		if !crr.completed {
			log.Fatal("logic error: RequestResponse should have been marked completed")
		}

		if crr.readOps > 0 && crr.writeOps > 0 {
			log.Fatal("logic error: got a request with mixed read and write ops")
		}

		if crr.readOps == 0 && crr.writeOps == 0 {
			log.Fatal("logic error: got a request with zero read and write ops")
		}

		// If burn-in is requested, check if it needs to be stopped. If
		// so, reset the timer and stop burn-in.
		if doingBurnIn {
			burnins += crr.readOps + crr.writeOps
			if burnins >= nBurnIn {
				doingBurnIn = false
				// Reset statistics timer when burn-in is complete:
				start = time.Now()
			}
		} else {
			if crr.writeOps > 0 {
				writeOps += crr.writeOps
				writeRequests++
			} else {
				readOps += crr.readOps
				readRequests++

				if len(crr.readkey) == 0 {
					log.Fatalf("logic error: empty readkey on completed read result: %+v", crr)
				}
			}
		}

		// If validation is configured, check if validation needs to be
		// stopped based on this response. If not, print the read
		// result. If validation needs to be stopped, stop doing
		// validation.
		if doingValidation {
			if crr.readOps > 0 {
				validations += crr.readOps
				fmt.Fprintf(validationWriter, "%s -> %s\n", crr.readkey, crr.readval)
				if validations >= nValidation {
					doingValidation = false
					validationWriter.Flush()
					validationFile.Close()
				}
			}
		}

		// Reset the RequestResponse and give it back to the Pool.
		crr.Reset()
		rrPool.Put(crr)
	}

	end := time.Now()
	tookNanos := float64(end.Sub(start).Nanoseconds())
	secs := tookNanos / 1e9

	fmt.Printf("benchmark complete:\n")
	fmt.Printf("  %d read commands logged for validation\n", validations)
	fmt.Printf("  %d commands executed before beginning statistics collection (burn-in)\n", burnins)
	fmt.Printf("  %d client requests executed\n", readRequests+writeRequests)
	fmt.Printf("  %d read commands\n", readOps)
	fmt.Printf("  %d write commands (possibly batched together)\n", writeOps)
	fmt.Printf("  %d total commands\n", readOps+writeOps)
	fmt.Printf("  %.1f write commands/sec\n", float64(writeOps)/secs)
	fmt.Printf("  %.1f read commands/sec\n", float64(readOps)/secs)
	fmt.Printf("  %d ns/read command\n", int64(tookNanos/float64(readOps)))
	fmt.Printf("  %d ns/write command\n", int64(tookNanos/float64(writeOps)))
}
