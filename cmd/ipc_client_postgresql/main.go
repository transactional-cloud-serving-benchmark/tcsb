package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"

	"github.com/google/flatbuffers/go"
	_ "github.com/lib/pq" // implicitly used by database/sql
	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

var schemaModes map[string]struct{} = map[string]struct{}{"simple_kv": struct{}{}}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.Uint64Flag{Name: "workers", Value: 1, Usage: "Number of parallel workers to use when submitting requests."},
		cli.StringFlag{Name: "schema-mode", Value: "", Usage: "One of: [simple_kv]."},
		cli.StringFlag{Name: "hosts", Value: "127.0.0.1", Usage: "Comma-separated value of server hostnames. The first hostname will be used to set up the database schema."},
		cli.IntFlag{Name: "port", Value: 5433, Usage: "DB port."},
		cli.StringFlag{Name: "user", Value: "postgres", Usage: "DB user."},
	}
	app.Action = func(c *cli.Context) error {
		schemaMode := c.String("schema-mode")
		if _, ok := schemaModes[schemaMode]; !ok {
			log.Fatalf("invalid schema-mode. choose from %v", schemaModes)
		}
		log.Printf("Schema mode: %v\n", schemaMode)

		hostsCsv := c.String("hosts")
		hosts := strings.Split(hostsCsv, ",")
		log.Printf("Parsed hostnames: %v\n", hosts)

		port := c.Int("port")
		log.Printf("Port: %d\n", port)

		nWorkers := c.Int("workers")
		log.Printf("Workers: %d\n", nWorkers)

		user := c.String("user")
		log.Printf("User: %s\n", user)

		run(schemaMode, hosts, port, user, nWorkers)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func run(schemaMode string, hosts []string, port int, user string, nWorkers int) {
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

	pgClient := NewPostgreSQLClient(schemaMode, hosts, port, user)
	pgClient.Setup()
	defer pgClient.Teardown()

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

				pgClient.HandleRequestResponse(builder, bufpPool, cpi.req)
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

type PostgreSQLClient struct {
	schemaMode string

	hosts []string
	port  int
	user  string

	dbs []*sql.DB
}

func NewPostgreSQLClient(schemaMode string, hosts []string, port int, user string) *PostgreSQLClient {
	return &PostgreSQLClient{
		schemaMode: schemaMode,

		hosts: hosts,
		port:  port,
		user:  user,

		dbs: nil,
	}
}

func (psc *PostgreSQLClient) Setup() {
	{
		// Open connection for database administration (does not use full connection string).
		log.Println("Connecting to database cluster with one connection, to drop and recreate database")
		connStrForSetup := fmt.Sprintf("host=%s port=%d user=%s sslmode=disable", psc.hosts[0], psc.port, psc.user)
		db, err := sql.Open("postgres", connStrForSetup)
		if err != nil {
			log.Fatal("failed to open database: ", err)
		}

		// Drop logical database.
		_, err = db.Exec("DROP DATABASE IF EXISTS tcsb")
		if err != nil {
			log.Fatal(err)
		}

		// Create logical database.
		_, err = db.Exec("CREATE DATABASE tcsb")
		if err != nil {
			log.Fatal(err)
		}
	}
	{
		// Open connection for database administration (uses full connection string).
		log.Println("Connecting to database cluster with one connection, to populate schema")
		connStrForSetup := fmt.Sprintf("host=%s port=%d user=%s dbname=tcsb sslmode=disable", psc.hosts[0], psc.port, psc.user)
		db, err := sql.Open("postgres", connStrForSetup)
		if err != nil {
			log.Fatal("failed to open database: ", err)
		}

		// Create table.
		if psc.schemaMode == "simple_kv" {
			_, err = db.Exec("CREATE TABLE keyvalue (key VARCHAR PRIMARY KEY, val VARCHAR)")
		} else {
			panic("logic error: unknown schema mode")
		}
		if err != nil {
			log.Fatal(err)
		}
	}

	psc.dbs = make([]*sql.DB, 0)
	for _, host := range psc.hosts {
		connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=tcsb sslmode=disable", host, psc.port, psc.user)
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Fatal("failed to open database: ", err)
		}
		psc.dbs = append(psc.dbs, db)
	}
}

func (psc *PostgreSQLClient) Teardown() {
	for _, db := range psc.dbs {
		db.Close()
	}
}

func (psc *PostgreSQLClient) HandleRequestResponse(builder *flatbuffers.Builder, bufpPool *sync.Pool, req serialized_messages.Request) {
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

		// Begin PostgreSQL-specific read logic.
		// Choose a random host (keepalive/connection pooling happens like normal within each connection).
		db := psc.dbs[rand.Intn(len(psc.dbs))]
		rows, err := db.Query("SELECT val FROM tcsb.keyvalue WHERE key = $1 LIMIT 1", rr.KeyBytes())
		err = rows.Scan(valbufp)
		if err != nil {
			log.Fatal("error during scanning of read query result", err)
		}
		// End PostgreSQL-specific query logic.

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

		// Begin PostgreSQL-specific write logic.
		db := psc.dbs[rand.Intn(len(psc.dbs))]
		txn, err := db.Begin()
		if err != nil {
			log.Fatal("error when creating a transaction")
		}
		for i := 0; i < bwr.KeyValuePairsLength(); i++ {
			kvp := serialized_messages.KeyValuePair{}
			bwr.KeyValuePairs(&kvp, i)

			_, err = txn.Exec(`INSERT INTO tcsb.keyvalue (key, val) VALUES ($1, $2)`, kvp.KeyBytes(), kvp.ValueBytes())
			if err != nil {
				txn.Rollback()
				log.Fatal("composing transaction failed", err)
			}
		}
		if err := txn.Commit; err != nil {
			log.Fatal("write transaction failed", err)
		}
		// End PostgreSQL-specific write logic.

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
