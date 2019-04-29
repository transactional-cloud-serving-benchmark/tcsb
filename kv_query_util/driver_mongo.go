package kv_query_util

import (
	"context"
	"log"
	"sync"
	"time"

	bson "go.mongodb.org/mongo-driver/bson"
	bson_primitive "go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
	mongo_readpref "go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type MongoDriver struct {
	address        string
	client         *mongo.Client
	collection     *mongo.Collection
	writeBatchPool *sync.Pool
}

func NewMongoDriver(address string) *MongoDriver {
	return &MongoDriver{
		address: address,
		writeBatchPool: &sync.Pool{
			New: func() interface{} {
				return []interface{}{}
			},
		},
	}
}

func (md *MongoDriver) Setup() {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, mongo_options.Client().ApplyURI(md.address))
	if err != nil {
		log.Fatal(err)
	}

	// Block waiting for a response from the server.
	err = client.Ping(context.Background(), mongo_readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	// Create a handle to the collection we will benchmark.
	// Creates the database and collection if needed.
	collection := client.Database("tcsb").Collection("kv")

	// Assert that the collection is empty.
	count, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	if count != 0 {
		log.Fatal("mongo target collection is not empty")
	}

	md.client = client
	md.collection = collection
}

func (md *MongoDriver) ExecuteOnce(rr *RequestResponse) {
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

		res := md.collection.FindOne(context.Background(), bson.D{{"_id", scratchCmd.KeyBytes()}})
		if err := res.Err(); err != nil {
			log.Fatal("FindOne: ", err)
		}

		ret := bson.D{{"_id", ""}, {"val", ""}}

		rr.readval = rr.readval[:0]

		// Ignore decode errors, because we want to leave rr.readval
		// empty if there was a decode error:
		err := res.Decode(&ret)
		if err != nil {
			// Errors are okay here. We handle them by printing
			// blank values in the validation results, if
			// applicable.
		}

		// Parse the value from the response.
		if err == nil {
			switch x := ret[1].Value.(type) {
			case bson_primitive.Binary:
				rr.readval = append(rr.readval, x.Data...)
			case string:
				rr.readval = append(rr.readval, []byte(x)...)
			}
		}

		rr.readOps = n
	} else if rr.op == serialized_messages.KeyValueCommandOpWrite {
		writeBatch := md.writeBatchPool.Get().([]interface{})
		writeBatch = writeBatch[:0]
		for i := 0; i < rr.batchCmd.CommandsLength(); i++ {
			scratchCmd := serialized_messages.KeyValueCommand{}
			rr.batchCmd.Commands(&scratchCmd, i)
			writeBatch = append(writeBatch, bson.M{"_id": scratchCmd.KeyBytes(), "val": scratchCmd.ValueBytes()})
		}

		imo := mongo_options.InsertMany()
		// Disable document validation to improve speed.
		imo.SetBypassDocumentValidation(false)
		// Disable ordered bulk writes to improve speed.
		imo.SetOrdered(false)

		_, err := md.collection.InsertMany(context.Background(), writeBatch, imo)
		if err != nil {
			log.Fatal("InsertMany: ", err)
		}

		writeBatch = writeBatch[:0]
		md.writeBatchPool.Put(writeBatch)

		rr.writeOps = n
	} else {
		log.Fatal("logic error: unrecognized op value found")
	}

	rr.completed = true
}

func (md *MongoDriver) Teardown() {
}
