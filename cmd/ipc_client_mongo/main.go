package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/flatbuffers/go"
	cli "gopkg.in/urfave/cli.v1"

	bson "go.mongodb.org/mongo-driver/bson"
	bson_primitive "go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
	mongo_readpref "go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "address", Value: "mongodb://127.0.0.1:27017", Usage: "server address"},
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

	mongoClient := NewMongoClient(address)
	mongoClient.Setup()
	defer mongoClient.Teardown()

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

				mongoClient.HandleRequestResponse(builder, bufpPool, cpi.req)
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

type MongoClient struct {
	address             string
	client              *mongo.Client
	collection          *mongo.Collection
	mongoWriteBatchPool *sync.Pool
}

func NewMongoClient(address string) *MongoClient {
	return &MongoClient{
		address: address,
		mongoWriteBatchPool: &sync.Pool{
			New: func() interface{} {
				return []interface{}{}
			},
		},
	}
}

func (mc *MongoClient) Setup() {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, mongo_options.Client().ApplyURI(mc.address))
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

	mc.client = client
	mc.collection = collection
}

func (mc *MongoClient) HandleRequestResponse(builder *flatbuffers.Builder, bufpPool *sync.Pool, req serialized_messages.Request) {
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
		// Begin Mongo-specific read logic.
		///////////////////////////////////
		res := mc.collection.FindOne(context.Background(), bson.D{{"_id", rr.KeyBytes()}})
		if err := res.Err(); err != nil {
			log.Fatal("FindOne: ", err)
		}

		ret := bson.D{{"_id", ""}, {"val", ""}}

		// Ignore decode errors, because we want to leave rr.readval
		// empty if there was a decode error:
		err := res.Decode(&ret)
		if err != nil {
			// Errors are acceptable here. We handle them by
			// printing blank values in the validation results, if
			// applicable.
		}

		// Parse the value from the response.
		if err == nil {
			switch x := ret[1].Value.(type) {
			case bson_primitive.Binary:
				*valbufp = append(*valbufp, x.Data...)
			case string:
				*valbufp = append(*valbufp, []byte(x)...)
			}
		}
		/////////////////////////////////
		// End Mongo-specific read logic.
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
		// Begin Mongo-specific write logic.
		////////////////////////////////////
		mongoWriteBatch := mc.mongoWriteBatchPool.Get().([]interface{})
		mongoWriteBatch = mongoWriteBatch[:0]
		for i := 0; i < bwr.KeyValuePairsLength(); i++ {
			kvp := serialized_messages.KeyValuePair{}
			bwr.KeyValuePairs(&kvp, i)
			mongoWriteBatch = append(mongoWriteBatch, bson.M{"_id": kvp.KeyBytes(), "val": kvp.ValueBytes()})
		}
		imo := mongo_options.InsertMany()
		// Disable document validation to improve speed.
		imo.SetBypassDocumentValidation(true)
		// Disable ordered bulk writes to improve speed.
		imo.SetOrdered(false)

		_, err := mc.collection.InsertMany(context.Background(), mongoWriteBatch, imo)
		if err != nil {
			log.Fatal("InsertMany: ", err)
		}

		mongoWriteBatch = mongoWriteBatch[:0]
		mc.mongoWriteBatchPool.Put(mongoWriteBatch)

		//////////////////////////////////
		// End Mongo-specific write logic.
		//////////////////////////////////

		// Encode the batch write reply information for the IPC driver.
		serialization_util.EncodeBatchWriteReplyWithFraming(builder, uint64(bwr.KeyValuePairsLength()))
	} else {
		log.Fatal("logic error: invalid request type")
	}
}

func (mc *MongoClient) Teardown() {
}

type clientPoolInput struct {
	req  serialized_messages.Request
	bufp *[]byte
}
