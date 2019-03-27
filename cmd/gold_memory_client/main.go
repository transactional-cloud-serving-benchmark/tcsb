package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	cli "gopkg.in/urfave/cli.v1"
	"github.com/valyala/fasthttp"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialized_messages"
	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
)

func main() {
	app := cli.NewApp()
	app.Action = func(c *cli.Context) error {
		if c.NArg() != 2 {
			log.Printf("usage: %s <base_url> <validation>", c.App.Name)
			log.Printf("  base_url:   http://127.0.0.1:8080")
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
	start := time.Now()
	writes := 0
	reads := 0
	writeBatches := 0

	in := bufio.NewReader(os.Stdin)

	url := baseUrl + "/kvdb"

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
			if batchCmd.CommandsLength() != 1 {
				log.Fatal("bad data: non-singular read batch")
			}

			scratchCmd := serialized_messages.KeyValueCommand{}
			batchCmd.Commands(&scratchCmd, 0)
			req := fasthttp.AcquireRequest()
			req.SetRequestURI(url + "?key=" + string(scratchCmd.KeyBytes())) // alloc

			resp := fasthttp.AcquireResponse()
			err := fasthttp.Do(req, resp)
			if err != nil {
				log.Fatal(err)
			}

			if validation {
				fmt.Printf("%s -> %s\n", scratchCmd.KeyBytes(), resp.Body())
			}

			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)

			reads++

		} else {
			req := fasthttp.AcquireRequest()
			req.Header.SetMethod("POST")
			req.SetRequestURI(url)
			for i := 0; i < batchCmd.CommandsLength(); i++ {
				scratchCmd := serialized_messages.KeyValueCommand{}
				batchCmd.Commands(&scratchCmd, i)
				req.AppendBody(scratchCmd.KeyBytes())
				req.AppendBody([]byte("="))
				req.AppendBody(scratchCmd.ValueBytes())
				req.AppendBody([]byte("\n"))
				writes++
			}

			resp := fasthttp.AcquireResponse()
			err := fasthttp.Do(req, resp)
			if err != nil {
				log.Fatal(err)
			}

			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			writeBatches++
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

//func post(url string) {
//	req := fasthttp.AcquireRequest()
//	req.SetRequestURI(url)
//	req.Header.SetMethod("POST")
//	req.SetBodyString("p=q")
//
//	resp := fasthttp.AcquireResponse()
//	client := &fasthttp.Client{}
//	client.Do(req, resp)
//
//	bodyBytes := resp.Body()
//	println(string(bodyBytes))
//	// User-Agent: fasthttp
//	// Body: p=q
//}
