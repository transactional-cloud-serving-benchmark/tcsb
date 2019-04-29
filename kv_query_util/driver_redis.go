package kv_query_util

import (
	"log"
	"unsafe"

	"github.com/go-redis/redis"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type RedisDriver struct {
	address string
	client  *redis.Client
}

func NewRedisDriver(address string) *RedisDriver {
	return &RedisDriver{
		address: address,
	}
}

func (rd *RedisDriver) Setup() {
	client := redis.NewClient(&redis.Options{
		Addr: rd.address,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	rd.client = client
}

func (rd *RedisDriver) ExecuteOnce(rr *RequestResponse) {
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

		keyString := zeroAllocBytesToString(scratchCmd.KeyBytes())
		retval, err := rd.client.Get(keyString).Result()
		if err != nil {
			// This is okay, because we just show an empty
			// validation value to the user.
		}

		rr.readval = rr.readval[:0]
		rr.readval = append(rr.readval, retval...)

		rr.readOps = n
	} else if rr.op == serialized_messages.KeyValueCommandOpWrite {
		pipeline := rd.client.Pipeline()
		for i := 0; i < rr.batchCmd.CommandsLength(); i++ {
			scratchCmd := serialized_messages.KeyValueCommand{}
			rr.batchCmd.Commands(&scratchCmd, i)
			keyString := zeroAllocBytesToString(scratchCmd.KeyBytes())
			pipeline.Set(keyString, scratchCmd.ValueBytes(), 0)
		}
		if _, err := pipeline.Exec(); err != nil {
			log.Fatal("write batch: ", err)
		}

		rr.writeOps = n
	} else {
		log.Fatal("logic error: unrecognized op value found")
	}
	rr.completed = true
}

func (rd *RedisDriver) Teardown() {
	rd.client.Close()
}

func zeroAllocBytesToString(x []byte) string {
	return *(*string)(unsafe.Pointer(&x))
}
