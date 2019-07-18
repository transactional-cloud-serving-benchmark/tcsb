package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/flatbuffers/go"
	_ "github.com/lib/pq" // implicitly used by database/sql
	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

const (
	SubScenarioKV                         string = "kv"
	SubScenarioKVWithSecondaryIndexLookup string = "kv_by_value"
)

var schemaModes map[string]struct{} = map[string]struct{}{SubScenarioKV: struct{}{}, SubScenarioKVWithSecondaryIndexLookup: struct{}{}}

var logger *log.Logger

func main() {

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.Uint64Flag{Name: "workers", Value: 1, Usage: "Number of parallel workers to use when submitting requests."},
		cli.StringFlag{Name: "schema-mode", Value: "", Usage: "One of: [kv, kv_by_value]."},
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
		log.Fatal("toplevel error", err)
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

	pgClient := NewPostgreSQLClient(schemaMode, hosts, port, user, nWorkers)
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
		go func(workerId int) {
			nRequests := 0
			builder := flatbuffers.NewBuilder(4096)
			for cpi := range clientPoolInputs {
				builder.Reset()

				pgClient.HandleRequestResponse(builder, bufpPool, cpi.req, workerId, nRequests)
				if len(builder.FinishedBytes()) == 0 {
					log.Fatal("bad reply serialization")
				}

				*cpi.bufp = (*cpi.bufp)[:0]
				if cap(*cpi.bufp) < 4096 {
					log.Fatal("logic error: cpi.bufp is too small")
				}
				bufpPool.Put(cpi.bufp)
				cpi.bufp = nil

				// The Builder contains the output bytes, so
				// copy the data and send a new bufp along.
				bufp := bufpPool.Get().(*[]byte)
				if cap(*bufp) < 4096 {
					log.Fatal("logic error: bufp is too small")
				}
				if cap(*bufp) < len(builder.FinishedBytes()) {
					log.Fatalf("%d vs %d", cap(*bufp), len(builder.FinishedBytes()))
				}
				(*bufp) = (*bufp)[:len(builder.FinishedBytes())]
				copy(*bufp, builder.FinishedBytes())

				outputBufps <- bufp
				nRequests++
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(outputBufps)
	}()

	for outputBufp := range outputBufps {
		out.Write(*outputBufp)

		*outputBufp = (*outputBufp)[:0]
		if cap(*outputBufp) < 4096 {
			log.Fatal("logic error: outputBufp is too small")
		}
		bufpPool.Put(outputBufp)
	}
}

type PostgreSQLClient struct {
	schemaMode string
	nWorkers   int

	hosts []string
	port  int
	user  string

	dbs []*sql.DB

	conns [][]*sql.Conn

	preparedInsertStatements, preparedSelectStatements map[*sql.Conn]*sql.Stmt
}

func NewPostgreSQLClient(schemaMode string, hosts []string, port int, user string, nWorkers int) *PostgreSQLClient {
	return &PostgreSQLClient{
		schemaMode: schemaMode,
		nWorkers:   nWorkers,

		hosts: hosts,
		port:  port,
		user:  user,

		dbs:   nil,
		conns: nil,

		preparedInsertStatements: nil,
		preparedSelectStatements: nil,
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

		// Use a single connection, not the connection pool, to mitigate distributed logic issues when manipulating schemas.
		conn, err := db.Conn(context.Background())
		if err != nil {
			log.Fatal("error when establishing conn for schema manipulation: ", err)
		}

		// Create logical database (if it already exists, move on).
		_, err = conn.ExecContext(context.Background(), "CREATE DATABASE tcsb")
		if err != nil {
			log.Println("recoverable error when (re-)creating database", err)
		}

		if err = conn.Close(); err != nil {
			log.Fatal("error when closing schema manipulation connection", err)
		}
		if err = db.Close(); err != nil {
			log.Fatal("error when closing database connection: ", err)
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

		// Use a single connection, not the connection pool, to mitigate distributed logic issues when manipulating schemas.
		conn, err := db.Conn(context.Background())
		if err != nil {
			log.Fatal("error when establishing conn for schema manipulation: ", err)
		}

		// Create table.
		if psc.schemaMode == SubScenarioKV {
			if _, err = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS keyvalue"); err != nil {
				log.Fatal("error when dropping table: ", err)
			}

			if _, err = conn.ExecContext(context.Background(), "CREATE TABLE keyvalue (key VARCHAR PRIMARY KEY, val VARCHAR)"); err != nil {
				log.Fatal("error when creating table: ", err)
			}
			if _, err = conn.ExecContext(context.Background(), "TRUNCATE TABLE keyvalue"); err != nil {
				log.Fatal("recoverable error when truncating table: ", err)
			}

		} else if psc.schemaMode == SubScenarioKVWithSecondaryIndexLookup {
			if _, err = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS keyvalue"); err != nil {
				log.Fatal("error when dropping table: ", err)
			}

			if _, err = conn.ExecContext(context.Background(), "CREATE TABLE keyvalue (key VARCHAR PRIMARY KEY, val VARCHAR)"); err != nil {
				log.Fatal("error when creating table: ", err)
			}
			if _, err = conn.ExecContext(context.Background(), "CREATE INDEX ON keyvalue (val)"); err != nil {
				log.Fatal("error when creating index: ", err)
			}
			if _, err = conn.ExecContext(context.Background(), "TRUNCATE TABLE keyvalue"); err != nil {
				log.Fatalf("error when truncating table: ", err)
			}
		} else {
			panic("logic error: unknown schema mode")
		}
		if err = conn.Close(); err != nil {
			log.Fatal("error when closing schema manipulation connection", err)
		}
		if err = db.Close(); err != nil {
			log.Fatal("error when closing database connection: ", err)
		}
	}

	log.Println("schema setup complete")

	psc.dbs = make([]*sql.DB, 0)
	psc.preparedInsertStatements = make(map[*sql.Conn]*sql.Stmt)
	psc.preparedSelectStatements = make(map[*sql.Conn]*sql.Stmt)
	for _, host := range psc.hosts {
		connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=tcsb sslmode=disable", host, psc.port, psc.user)
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Fatal("failed to open database: ", err)
		}

		psc.dbs = append(psc.dbs, db)
	}

	psc.conns = make([][]*sql.Conn, 0, psc.nWorkers)
	nConns := 0
	for i := 0; i < psc.nWorkers; i++ {
		workerConns := make([]*sql.Conn, 0, len(psc.hosts))
		for _, db := range psc.dbs {
			c, err := db.Conn(context.Background())
			if err != nil {
				log.Fatal("establishing conn: ", err)
			}
			c.PingContext(context.Background())
			workerConns = append(workerConns, c)
			nConns++

			var insertStmt, selectStmt *sql.Stmt

			if psc.schemaMode == SubScenarioKV {
				insertStmt, err = c.PrepareContext(context.Background(), `INSERT INTO keyvalue (key, val) VALUES ($1, $2)`)
				if err != nil {
					log.Fatal("failed to prepare insert statement: ", err)
				}
				selectStmt, err = c.PrepareContext(context.Background(), `SELECT val FROM keyvalue WHERE key = $1 LIMIT 1`)
				if err != nil {
					log.Fatal("failed to prepare select statement: ", err)
				}
			} else if psc.schemaMode == SubScenarioKVWithSecondaryIndexLookup {
				insertStmt, err = c.PrepareContext(context.Background(), `INSERT INTO keyvalue (key, val) VALUES ($1, $2)`)
				if err != nil {
					log.Fatal("failed to prepare insert statement: ", err)
				}
				selectStmt, err = c.PrepareContext(context.Background(), `SELECT key FROM keyvalue WHERE val = $1 LIMIT 1`)
				if err != nil {
					log.Fatal("failed to prepare select statement: ", err)
				}
			} else {
				log.Fatal("unknown schema mode")
			}

			psc.preparedInsertStatements[c] = insertStmt
			psc.preparedSelectStatements[c] = selectStmt
		}
		psc.conns = append(psc.conns, workerConns)
		if i > 0 && i%10 == 0 {
			log.Printf("%d workers set up", i)
		}
	}
	log.Printf("connections established: %d", nConns)
}

func (psc *PostgreSQLClient) Teardown() {
	for _, db := range psc.dbs {
		db.Close()
	}
}

func (psc *PostgreSQLClient) HandleRequestResponse(builder *flatbuffers.Builder, bufpPool *sync.Pool, req serialized_messages.Request, workerId, nRequests int) {
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

		myConns := psc.conns[workerId]
		conn := myConns[nRequests%len(myConns)]
		preparedStmt := psc.preparedSelectStatements[conn]
		start := time.Now()
		rows, err := preparedStmt.Query(rr.KeyBytes())
		if err != nil {
			log.Fatal("error during SELECT query: ", err)
		}
		rows.Next()
		// N.B. calling rows.Scan on a []byte slice causes the sql library to malloc a new []byte object.
		valbufpRawBytes := (*sql.RawBytes)(valbufp)
		rows.Scan(valbufpRawBytes)
		rows.Close()
		latencyNanos := uint64(time.Since(start).Nanoseconds())
		//if ok := rows.Next(); !ok {
		//	log.Fatal("error during calling Next on Rows of read query result", rows.Err())
		//}
		//err = rows.Scan(valbufp)
		//if err != nil {
		//	log.Fatal("error during scanning of read query result", err)
		//}
		// End PostgreSQL-specific query logic.

		// Encode the read reply information for the IPC driver.
		serialization_util.EncodeReadReplyWithFraming(builder, rr.KeyBytes(), *valbufp, latencyNanos)

		// Reset and store the bufp.
		*valbufp = (*valbufp)[:0]
		if cap(*valbufp) < 4096 {
			log.Fatalf("logic error, valbufp cap is %d", cap(*valbufp))
		}
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
		myConns := psc.conns[workerId]
		conn := myConns[nRequests%len(myConns)]
		start := time.Now()
		if psc.schemaMode == SubScenarioKV || psc.schemaMode == SubScenarioKVWithSecondaryIndexLookup {
			if bwr.KeyValuePairsLength() != 1 {
				log.Fatal("kv needs writes of size 1")
			}
			kvp := serialized_messages.KeyValuePair{}
			bwr.KeyValuePairs(&kvp, 0)
			preparedInsertStmt := psc.preparedInsertStatements[conn]
			if _, err := preparedInsertStmt.Exec(kvp.KeyBytes(), kvp.ValueBytes()); err != nil {
				log.Fatal("write transaction failed", err)
			}
		} else {
			// This code path is temporarily disabled while we change how we do schema setup.
			log.Fatal("unsupported batch-transactional insert mode")
			// preparedInsertStmt := psc.preparedInsertStatements[conn]
			// txn, err := conn.BeginTx(context.Background(), nil)
			// if err != nil {
			// 	log.Fatal("error when creating a transaction")
			// }
			// for i := 0; i < bwr.KeyValuePairsLength(); i++ {
			// 	if i > 0 {
			// 	}
			// 	kvp := serialized_messages.KeyValuePair{}
			// 	bwr.KeyValuePairs(&kvp, i)

			// 	_, err = txn.Stmt(preparedInsertStmt).Exec(kvp.KeyBytes(), kvp.ValueBytes())
			// 	if err != nil {
			// 		txn.Rollback()
			// 		log.Fatal("composing transaction failed", err)
			// 	}
			// }
			// if err := txn.Commit(); err != nil {
			// 	log.Fatal("write transaction failed", err)
			// }
		}
		// End PostgreSQL-specific write logic.
		latencyNanos := uint64(time.Since(start).Nanoseconds())

		// Encode the batch write reply information for the IPC driver.
		serialization_util.EncodeBatchWriteReplyWithFraming(builder, uint64(bwr.KeyValuePairsLength()), latencyNanos)
	} else {
		log.Fatal("logic error: invalid request type")
	}
}

type clientPoolInput struct {
	req  serialized_messages.Request
	bufp *[]byte
}
