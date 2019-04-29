package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/kv_query_util"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "addresses", Value: "127.0.0.1", Usage: "comma-separated value of server addresses"},
		cli.Uint64Flag{Name: "validation", Value: 0, Usage: "Number of read responses to print for validation purposes."},
		cli.StringFlag{Name: "validation-filename", Value: "/dev/stderr", Usage: "Destination file to write validation results, if applicable."},
		cli.Uint64Flag{Name: "burn-in", Value: 0, Usage: "Number of read/write requests to use for burn-in before collecting statistics."},
		cli.Uint64Flag{Name: "workers", Value: 1, Usage: "Number of parallel workers to use when submitting requests."},
	}
	app.Action = func(c *cli.Context) error {
		addressesCsv := c.String("addresses")
		addresses := strings.Split(addressesCsv, ",")
		fmt.Printf("Parsed addresses: %v\n", addresses)

		nValidation := c.Uint64("validation")
		fmt.Printf("Validation: %d\n", nValidation)
		validationFilename := c.String("validation-filename")
		fmt.Printf("Validation filename: %s\n", validationFilename)

		nBurnIn := c.Uint64("burn-in")
		fmt.Printf("Burn-in: %d\n", nBurnIn)

		nWorkers := c.Int("workers")
		fmt.Printf("Workers: %d\n", nWorkers)

		driver := kv_query_util.NewCassandraDriver(addresses)
		kv_query_util.Run(driver, nValidation, validationFilename, nBurnIn, nWorkers)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
