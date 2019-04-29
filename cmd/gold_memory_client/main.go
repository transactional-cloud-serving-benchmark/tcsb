package main

import (
	"fmt"
	"log"
	"os"

	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/kv_query_util"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "address", Value: "127.0.0.1:8000", Usage: "Address of gold-memory server."},
		cli.Uint64Flag{Name: "validation", Value: 0, Usage: "Number of read responses to print for validation purposes."},
		cli.StringFlag{Name: "validation-filename", Value: "/dev/stderr", Usage: "Destination file to write validation results, if applicable."},
		cli.Uint64Flag{Name: "burn-in", Value: 0, Usage: "Number of read/write requests to use for burn-in before collecting statistics."},
		cli.Uint64Flag{Name: "workers", Value: 1, Usage: "Number of parallel workers to use when submitting requests."},
	}
	app.Action = func(c *cli.Context) error {
		address := c.String("address")
		fmt.Printf("Address: %v\n", address)

		nValidation := c.Uint64("validation")
		fmt.Printf("Validation: %d\n", nValidation)
		validationFilename := c.String("validation-filename")
		fmt.Printf("Validation filename: %s\n", validationFilename)

		nBurnIn := c.Uint64("burn-in")
		fmt.Printf("Burn-in: %d\n", nBurnIn)

		nWorkers := c.Int("workers")
		fmt.Printf("Workers: %d\n", nWorkers)

		driver := kv_query_util.NewGoldMemoryDriver(address)
		kv_query_util.Run(driver, nValidation, validationFilename, nBurnIn, nWorkers)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
