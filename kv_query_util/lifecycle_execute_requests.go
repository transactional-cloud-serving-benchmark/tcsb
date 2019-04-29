package kv_query_util

import (
	"sync"
)

func Run(driver Driver, nValidation uint64, validationFilename string, nBurnIn uint64, nWorkers int) {
	driver.Setup()
	defer driver.Teardown()

	rrPool := &sync.Pool{
		New: func() interface{} {
			return NewRequestResponse()
		},
	}

	rrs := make(chan RequestResponse, 1000)
	go func() {
		DispatchRequestsToWorkers(rrPool, rrs)
		close(rrs)
	}()

	completedRrs := make(chan RequestResponse, 1000)
	wg0 := &sync.WaitGroup{}
	for i := 0; i < nWorkers; i++ {
		wg0.Add(1)
		go func() {
			for rr := range rrs {
				driver.ExecuteOnce(&rr)
				completedRrs <- rr

			}
			wg0.Done()
		}()
	}

	wg1 := &sync.WaitGroup{}
	wg1.Add(1)
	go func() {
		CollectResponsesAndPrintResults(nBurnIn, nValidation, validationFilename, rrPool, completedRrs)
		wg1.Done()
	}()

	wg0.Wait()
	close(completedRrs)

	wg1.Wait()
}
