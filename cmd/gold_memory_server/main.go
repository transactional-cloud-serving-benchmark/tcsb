package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	cli "gopkg.in/urfave/cli.v1"
)

func main() {
	app := cli.NewApp()
	app.Action = func(c *cli.Context) error {
		if c.NArg() != 1 {
			log.Printf("usage: %s <store_mode>", c.App.Name)
			log.Printf("  store_mode: {full, skip}")
			log.Fatalf("exiting with error")
		}

		storeMode := c.Args().Get(0)
		if storeMode != "full" && storeMode != "skip" {
			log.Fatal("invalid store_mode, choose from {full, skip}")
		}

		serve(storeMode)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func serve(storeMode string) {
	doFullStorage := storeMode == "full"

	var atomicNumWrites uint64 = 0
	var atomicNumReads uint64 = 0

	var lastNumWrites uint64
	var lastNumReads uint64
	go func() {
		i := 0
		for {
			nw := atomic.LoadUint64(&atomicNumWrites)
			nr := atomic.LoadUint64(&atomicNumReads)

			if i == 0 || nw != lastNumWrites || nr != lastNumReads {
				lastNumWrites = nw
				lastNumReads = nr
				log.Printf("writes: %9d, reads: %9d", nw, nr)
			}

			i++

			time.Sleep(1 * time.Second)
		}
	}()
	m := &sync.Map{}

	gin.SetMode("release")
	r := gin.New()

	r.POST("/kvdb", func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(400, gin.H{
				"message": "missing body",
			})
 			return
		}
		lines := bytes.Split(body, []byte("\n"))
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			fields := bytes.Split(line, []byte("="))
			key := string(fields[0]) // alloc
			val := fields[1]
			atomic.AddUint64(&atomicNumWrites, 1)
			if doFullStorage {
				m.Store(key, val)
			}
		}
		c.JSON(201, gin.H{
			"message": "ok!",
		})
	})
	r.GET("/kvdb", func(c *gin.Context) {
		atomic.AddUint64(&atomicNumReads, 1)
		key := c.Query("key")
		if key == "" {
			c.JSON(400, gin.H{
				"message": "missing key arg",
			})
			return
		}
		interfaceVal, ok := m.Load(key)
		if !ok {
			c.JSON(404, gin.H{
				"message": "key not found",
			})
			return
		}
		val := interfaceVal.([]byte)
		c.Data(200, "application/octet-stream", val)
	})
	r.Run() // listen and serve on 0.0.0.0:8080
}
