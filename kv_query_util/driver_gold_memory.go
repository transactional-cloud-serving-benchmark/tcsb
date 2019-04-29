package kv_query_util

import (
	"log"
	"sync"

	"github.com/valyala/fasthttp"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type GoldMemoryDriver struct {
	address       string
	url           string
	client        *fasthttp.Client
	byteSlicePool *sync.Pool
}

func NewGoldMemoryDriver(address string) *GoldMemoryDriver {
	return &GoldMemoryDriver{
		url:     address + "/kvdb",
		address: address,
		byteSlicePool: &sync.Pool{
			New: func() interface{} {
				return []byte{}
			},
		},
	}
}

func (gmd *GoldMemoryDriver) Setup() {
	gmd.client = &fasthttp.Client{}
}

func (gmd *GoldMemoryDriver) ExecuteOnce(rr *RequestResponse) {
	if rr.completed {
		log.Fatal("logic error on RequestResponse processing")
	}
	n := uint64(rr.batchCmd.CommandsLength())
	if rr.op == serialized_messages.KeyValueCommandOpRead {
		if n != 1 {
			log.Fatal("bad data: non-singular read batch")
		}

		scratchCmd := serialized_messages.KeyValueCommand{}
		rr.batchCmd.Commands(&scratchCmd, 0)

		rr.readkey = rr.readkey[:0]
		rr.readkey = append(rr.readkey, scratchCmd.KeyBytes()...)

		uriBuf := gmd.byteSlicePool.Get().([]byte)
		uriBuf = append(uriBuf, gmd.url...)
		uriBuf = append(uriBuf, "?key="...)
		uriBuf = append(uriBuf, scratchCmd.KeyBytes()...)

		req := fasthttp.AcquireRequest()
		req.SetRequestURIBytes(uriBuf)

		resp := fasthttp.AcquireResponse()
		err := gmd.client.Do(req, resp)
		if err != nil {
			log.Fatal(err)
		}

		rr.readval = rr.readval[:0]
		rr.readval = append(rr.readval, resp.Body()...)

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)

		uriBuf = uriBuf[:0]
		gmd.byteSlicePool.Put(uriBuf)

		rr.readOps = n
	} else if rr.op == serialized_messages.KeyValueCommandOpWrite {
		req := fasthttp.AcquireRequest()
		req.Header.SetMethod("POST")
		req.SetRequestURI(gmd.url)
		for i := 0; i < rr.batchCmd.CommandsLength(); i++ {
			scratchCmd := serialized_messages.KeyValueCommand{}
			rr.batchCmd.Commands(&scratchCmd, i)
			req.AppendBody(scratchCmd.KeyBytes())
			req.AppendBody([]byte("="))
			req.AppendBody(scratchCmd.ValueBytes())
			req.AppendBody([]byte("\n"))
		}

		resp := fasthttp.AcquireResponse()
		err := fasthttp.Do(req, resp)
		if err != nil {
			log.Fatal(err)
		}

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)

		rr.writeOps = n
	} else {
		log.Fatal("logic error: unrecognized op value found")
	}
	rr.completed = true
}

func (gmd *GoldMemoryDriver) Teardown() {
}
