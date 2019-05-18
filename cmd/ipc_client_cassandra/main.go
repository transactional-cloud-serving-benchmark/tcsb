package main

import (
	"bufio"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/google/flatbuffers/go"
	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "addresses", Value: "127.0.0.1", Usage: "comma-separated value of server addresses"},
		cli.Uint64Flag{Name: "workers", Value: 1, Usage: "Number of parallel workers to use when submitting requests."},
	}
	app.Action = func(c *cli.Context) error {
		addressesCsv := c.String("addresses")
		addresses := strings.Split(addressesCsv, ",")
		log.Printf("Parsed addresses: %v\n", addresses)

		nWorkers := c.Int("workers")
		log.Printf("Workers: %d\n", nWorkers)

		run(addresses, nWorkers)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func run(addresses []string, nWorkers int) {
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

	cassandraClient := NewCassandraClient(addresses)
	cassandraClient.Setup()
	defer cassandraClient.Teardown()

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

				cassandraClient.HandleRequestResponse(builder, bufpPool, cpi.req)
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

type CassandraClient struct {
	addresses []string
	session   *gocql.Session
}

func NewCassandraClient(addresses []string) *CassandraClient {
	return &CassandraClient{
		addresses: addresses,
		session:   nil,
	}
}

func (cc *CassandraClient) Setup() {
	cluster := gocql.NewCluster(cc.addresses...)
	cluster.Keyspace = "tcsb"
	log.Println("Connecting to cluster")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Creating keyspace tcsb, if needed")
	if err := session.Query("CREATE KEYSPACE IF NOT EXISTS tcsb").Exec(); err != nil {
		log.Fatal(err)
	}

	log.Println("Creating table tcsb.keyvalue, if needed")
	{
		const s = `CREATE TABLE IF NOT EXISTS tcsb.keyvalue (key blob PRIMARY KEY, val blob)`
		if err := session.Query(s).Exec(); err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Truncating table tcsb.keyvalue")
	if err := session.Query(`TRUNCATE TABLE tcsb.keyvalue`).Exec(); err != nil {
		log.Fatal(err)
	}

	{
		log.Printf("Asserting that the table tcsb.keyvalue is empty... ")
		m := map[string]interface{}{}
		if err := session.Query(`SELECT COUNT(*) FROM tcsb.keyvalue`).MapScan(m); err != nil {
			log.Fatal(err)
		}
		v, ok := m["count"]
		if !ok {
			log.Fatal("bad COUNT result")
		}
		if v.(int64) != 0 {
			log.Fatal("table was not empty")
		}
		log.Println("Success!")
	}

	cc.session = session
}

func (cc *CassandraClient) Teardown() {
	cc.session.Close()
}

func (cc *CassandraClient) HandleRequestResponse(builder *flatbuffers.Builder, bufpPool *sync.Pool, req serialized_messages.Request) {
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

		// Begin Cassandra-specific read logic.
		iter := cc.session.Query("SELECT val FROM tcsb.keyvalue WHERE key = ? LIMIT 1", rr.KeyBytes()).Iter()
		for iter.Scan(valbufp) {
			break
		}
		// End Cassandra-specific read logic.

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

		// Begin Cassandra-specific write logic.
		cassBatch := cc.session.NewBatch(gocql.LoggedBatch)
		for i := 0; i < bwr.KeyValuePairsLength(); i++ {
			kvp := serialized_messages.KeyValuePair{}
			bwr.KeyValuePairs(&kvp, i)

			cassBatch.Query(`INSERT INTO tcsb.keyvalue (key, val) VALUES (?, ?)`, kvp.KeyBytes(), kvp.ValueBytes())
		}
		if err := cc.session.ExecuteBatch(cassBatch); err != nil {
			log.Fatal("write batch: ", err)
		}
		// End Cassandra-specific write logic.

		// Encode the batch write reply information for the IPC driver.
		serialization_util.EncodeBatchWriteReplyWithFraming(builder, uint64(bwr.KeyValuePairsLength()))
	} else {
		log.Fatal("logic error: invalid request type")
	}
}

type clientPoolInput struct {
	req  serialized_messages.Request
	bufp *[]byte
}
