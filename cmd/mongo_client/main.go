package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	cli "gopkg.in/urfave/cli.v1"
	"go.mongodb.org/mongo-driver/mongo"
	mongo_options "go.mongodb.org/mongo-driver/mongo/options"
	mongo_readpref "go.mongodb.org/mongo-driver/mongo/readpref"
	bson "go.mongodb.org/mongo-driver/bson"
	bson_primitive "go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
)

func main() {
	app := cli.NewApp()
	app.Action = func(c *cli.Context) error {
		if c.NArg() != 2 {
			log.Printf("usage: %s <base_url> <validation>", c.App.Name)
			log.Printf("  base_url:   mongodb://127.0.0.1:27017")
			log.Printf("  validation: {on, off}")
			log.Fatalf("exiting with error")
		}

		baseUrl := c.Args().Get(0)
		validation := c.Args().Get(1)
		if validation != "on" && validation != "off" {
			log.Fatal("invalid validation, choose from {on, off}")
		}

		run(baseUrl, validation == "on")
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func run(baseUrl string, validation bool) {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	client, err := mongo.Connect(ctx, mongo_options.Client().ApplyURI(baseUrl))
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
	collection := client.Database("benchmarking").Collection("kv")

	// Assert that the collection is empty.
	count, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	if count != 0 {
		log.Fatal("mongo target collection is not empty")
	}

	imo := mongo_options.InsertMany()
	// Disable document validation to improve speed.
	imo.SetBypassDocumentValidation(false)
	// Disable ordered bulk writes to improve speed.
	imo.SetOrdered(false)

	start := time.Now()
	writes := 0
	reads := 0
	writeBatches := 0

	// For each binary payload on stdin, decode it and queue it for sending to the server.
	in := bufio.NewReader(os.Stdin)
	msgReader := serialization_util.NewMessageReader()
	for {
		batchCmd, op, err := msgReader.Next(in)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if op == serialized_messages.KeyValueCommandOpRead {
			reads++
			if batchCmd.CommandsLength() != 1 {
				log.Fatal("bad data: non-singular read batch")
			}

			scratchCmd := serialized_messages.KeyValueCommand{}
			batchCmd.Commands(&scratchCmd, 0)

			res := collection.FindOne(context.Background(), bson.D{{"_id", scratchCmd.KeyBytes()}})
			if err := res.Err(); err != nil {
				log.Fatal("FindOne: ", err)
			}

			ret := bson.D{{"_id", ""}, {"val", ""}}
			err := res.Decode(&ret)
			if err != nil {
				log.Fatal("DecodeBytes: ", err)
			}
			if validation {
				fmt.Printf("%s -> %s\n", ret[0].Value.(bson_primitive.Binary).Data, ret[1].Value.(bson_primitive.Binary).Data)
			}
		} else {
			writeBatches++
			writeBatch := make([]interface{}, 0, batchCmd.CommandsLength()) // alloc
			//writeBatch := make([]interface{}, batchCmd.CommandsLength()) // alloc
			//writeBatch := make([]mongo.WriteModel, batchCmd.CommandsLength()) // alloc
			for i := 0; i < batchCmd.CommandsLength(); i++ {
				scratchCmd := serialized_messages.KeyValueCommand{}
				batchCmd.Commands(&scratchCmd, i)
				// TODO(rw): data race if parallelized
				writeBatch = append(writeBatch, bson.M{"_id": scratchCmd.KeyBytes(), "val": scratchCmd.ValueBytes()})
				writes++
			}

			//_, err := collection.BulkWrite(context.Background(), writeBatch, bwo)
			_, err := collection.InsertMany(context.Background(), writeBatch, imo)
			if err != nil {
				log.Fatal("InsertMany: ", err)
			}
		}

	}

	end := time.Now()
	tookNanos := float64(end.Sub(start).Nanoseconds())
	secs := tookNanos / 1e9

	if !validation {
		fmt.Printf("benchmark complete:\n")
		fmt.Printf("  %d requests executed\n", reads + writeBatches)
		fmt.Printf("  %d read ops\n", reads)
		fmt.Printf("  %d write ops\n", writes)
		fmt.Printf("  %f writes/sec\n", float64(writes) / secs)
		fmt.Printf("  %f reads/sec\n", float64(reads) / secs)
		fmt.Printf("  %f ns/read\n", tookNanos / float64(reads))
		fmt.Printf("  %f ns/write\n", tookNanos / float64(writes))
	}
}
