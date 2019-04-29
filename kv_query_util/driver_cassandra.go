package kv_query_util

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
)

type CassandraDriver struct {
	addresses []string
	session   *gocql.Session
}

func NewCassandraDriver(addresses []string) *CassandraDriver {
	return &CassandraDriver{
		addresses: addresses,
		session:   nil,
	}
}
func (ce *CassandraDriver) Setup() {
	cluster := gocql.NewCluster(ce.addresses...)
	cluster.Keyspace = "tcsb"
	fmt.Println("Connecting to cluster")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Creating keyspace tcsb, if needed")
	if err := session.Query("CREATE KEYSPACE IF NOT EXISTS tcsb").Exec(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Creating table tcsb.keyvalue, if needed")
	{
		const s = `CREATE TABLE IF NOT EXISTS tcsb.keyvalue (key blob PRIMARY KEY, val blob)`
		if err := session.Query(s).Exec(); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Truncating table tcsb.keyvalue")
	if err := session.Query(`TRUNCATE TABLE tcsb.keyvalue`).Exec(); err != nil {
		log.Fatal(err)
	}

	{
		fmt.Printf("Asserting that the table tcsb.keyvalue is empty... ")
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
		fmt.Println("Success!")
	}

	ce.session = session
}

func (ce *CassandraDriver) ExecuteOnce(rr *RequestResponse) {
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

		iter := ce.session.Query("SELECT val FROM tcsb.keyvalue WHERE key = ? LIMIT 1", scratchCmd.KeyBytes()).Iter()
		for iter.Scan(&rr.readval) {
			break
		}
		rr.readOps = n
	} else if rr.op == serialized_messages.KeyValueCommandOpWrite {
		cassBatch := ce.session.NewBatch(gocql.LoggedBatch)
		for i := 0; i < rr.batchCmd.CommandsLength(); i++ {
			scratchCmd := serialized_messages.KeyValueCommand{}
			rr.batchCmd.Commands(&scratchCmd, i)
			cassBatch.Query(`INSERT INTO tcsb.keyvalue (key, val) VALUES (?, ?)`, scratchCmd.KeyBytes(), scratchCmd.ValueBytes())
		}
		if err := ce.session.ExecuteBatch(cassBatch); err != nil {
			log.Fatal("write batch: ", err)
		}

		rr.writeOps = n
	} else {
		log.Fatal("logic error: unrecognized op value found")
	}
	rr.completed = true
}

func (ce *CassandraDriver) Teardown() {
	ce.session.Close()
}
