package main

import (
	"bufio"
	"log"
	"os"
	"sync"
	"unsafe"

	"github.com/go-redis/redis"
	"github.com/google/flatbuffers/go"
	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "address", Value: "127.0.0.1:6379", Usage: "Address for redis server."},
		cli.Uint64Flag{Name: "workers", Value: 1, Usage: "Number of parallel workers to use when submitting requests."},
	}
	app.Action = func(c *cli.Context) error {
		address := c.String("address")
		log.Printf("Address: %v\n", address)

		nWorkers := c.Int("workers")
		log.Printf("Workers: %d\n", nWorkers)

		run(address, nWorkers)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func run(address string, nWorkers int) {
	bufpPool := &sync.Pool{
		New: func() interface{} {
			x := make([]byte, 0, 4096)
			return &x
		},
	}

	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	defer func() {
		out.Flush()
		os.Stdout.Close()
	}()

	redisClient := NewRedisClient(address)
	redisClient.Setup()
	defer redisClient.Teardown()

	clientPoolInputs := make(chan clientPoolInput, 1000)
	outputBufps := make(chan *[]byte, 1000)

	go func() {
		for {
			bufp := bufpPool.Get().(*[]byte)
			req, err := serialization_util.DecodeNextCommand(in, bufp)
			if err != nil {
				break
			}

			clientPoolInputs <- clientPoolInput{req, bufp}
		}
		close(clientPoolInputs)
	}()

	wg := &sync.WaitGroup{}

	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			builder := flatbuffers.NewBuilder(4096)
			for cpi := range clientPoolInputs {
				builder.Reset()

				redisClient.HandleRequestResponse(builder, bufpPool, cpi.req)
				if len(builder.FinishedBytes()) == 0 {
					log.Fatal("bad reply serialization")
				}

				*cpi.bufp = (*cpi.bufp)[:0]
				bufpPool.Put(cpi.bufp)
				cpi.bufp = nil

				// The Builder contains the output bytes, so
				// copy the data and send a new bufp along.
				bufp := bufpPool.Get().(*[]byte)
				(*bufp) = (*bufp)[:len(builder.FinishedBytes())]
				copy(*bufp, builder.FinishedBytes())

				outputBufps <- bufp
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(outputBufps)
	}()

	for outputBufp := range outputBufps {
		out.Write(*outputBufp)

		*outputBufp = (*outputBufp)[:0]
		bufpPool.Put(outputBufp)
	}
}

type RedisClient struct {
	address string
	client  *redis.Client
}

func NewRedisClient(address string) *RedisClient {
	return &RedisClient{
		address: address,
	}
}

func (rc *RedisClient) Setup() {
	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	rc.client = client
}

func (rc *RedisClient) HandleRequestResponse(builder *flatbuffers.Builder, bufpPool *sync.Pool, req serialized_messages.Request) {
	builder.Reset()

	if req.RequestUnionType() == serialized_messages.RequestUnionReadRequest {
		// Decode read request
		t := flatbuffers.Table{}
		if ok := req.RequestUnion(&t); !ok {
			log.Fatal("logic error: bad RequestUnion decoding")
		}

		rr := serialized_messages.ReadRequest{}
		rr.Init(t.Bytes, t.Pos)

		if len(rr.KeyBytes()) == 0 {
			log.Fatal("missing keybytes")
		}

		valbufp := bufpPool.Get().(*[]byte)

		///////////////////////////////////
		// Begin Redis-specific read logic.
		///////////////////////////////////
		keyString := zeroAllocBytesToString(rr.KeyBytes())
		retval, err := rc.client.Get(keyString).Result()
		if err != nil {
			// This is okay, because we just show an empty
			// validation value to the user.
		}
		*valbufp = append(*valbufp, []byte(retval)...)
		/////////////////////////////////
		// End Redis-specific read logic.
		/////////////////////////////////

		// Encode the read reply information for the IPC driver.
		serialization_util.EncodeReadReplyWithFraming(builder, rr.KeyBytes(), *valbufp)

		// Reset and store the bufp.
		*valbufp = (*valbufp)[:0]
		bufpPool.Put(valbufp)
	} else if req.RequestUnionType() == serialized_messages.RequestUnionBatchWriteRequest {
		// Decode batch write request:
		t := flatbuffers.Table{}
		if ok := req.RequestUnion(&t); !ok {
			log.Fatal("logic error: bad RequestUnion decoding")
		}

		bwr := serialized_messages.BatchWriteRequest{}
		bwr.Init(t.Bytes, t.Pos)

		////////////////////////////////////
		// Begin Redis-specific write logic.
		////////////////////////////////////

		pipeline := rc.client.Pipeline()
		for i := 0; i < bwr.KeyValuePairsLength(); i++ {
			kvp := serialized_messages.KeyValuePair{}
			bwr.KeyValuePairs(&kvp, i)
			keyString := zeroAllocBytesToString(kvp.KeyBytes())
			pipeline.Set(keyString, kvp.ValueBytes(), 0)
		}
		if _, err := pipeline.Exec(); err != nil {
			log.Fatal("write batch: ", err)
		}

		//////////////////////////////////
		// End Redis-specific write logic.
		//////////////////////////////////

		// Encode the batch write reply information for the IPC driver.
		serialization_util.EncodeBatchWriteReplyWithFraming(builder, uint64(bwr.KeyValuePairsLength()))
	} else {
		log.Fatal("logic error: invalid request type")
	}
}

func (rc *RedisClient) Teardown() {
	rc.client.Close()
}

func zeroAllocBytesToString(x []byte) string {
	return *(*string)(unsafe.Pointer(&x))
}

type clientPoolInput struct {
	req  serialized_messages.Request
	bufp *[]byte
}
