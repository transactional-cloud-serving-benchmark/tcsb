package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/kv_query_util"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "cmd", Value: "", Usage: `Command, with arguments, to invoke as IPC child process. Example: "./artifacts/simple_ram_client --validation=100"`},
		cli.Uint64Flag{Name: "validation", Value: 0, Usage: "Number of read responses to print for validation purposes."},
		cli.StringFlag{Name: "validation-filename", Value: "/dev/stderr", Usage: "Destination file to write validation results, if applicable."},
		cli.Uint64Flag{Name: "burn-in", Value: 0, Usage: "Number of read/write requests to use for burn-in before collecting statistics."},
	}
	app.Action = func(c *cli.Context) error {
		nValidation := c.Uint64("validation")
		fmt.Printf("Validation: %d\n", nValidation)
		validationFilename := c.String("validation-filename")
		fmt.Printf("Validation filename: %s\n", validationFilename)

		nBurnIn := c.Uint64("burn-in")
		fmt.Printf("Burn-in: %d\n", nBurnIn)

		cmdString := c.String("cmd")
		fmt.Printf("cmd: %s\n", cmdString)
		if cmdString == "" {
			log.Fatal("missing cmd")
		}

		cmdFields := strings.Fields(cmdString)
		cmd := exec.Command(cmdFields[0], cmdFields[1:]...)

		ipcStdin, err := cmd.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}
		ipcStdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Fatal(err)
		}
		ipcStderr, err := cmd.StderrPipe()
		if err != nil {
			log.Fatal(err)
		}

		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}

		driver := kv_query_util.NewIPCDriver(ipcStdin, ipcStdout, ipcStderr)

		kv_query_util.RunIPCDriver(driver, nValidation, validationFilename, nBurnIn)

		if err := cmd.Wait(); err != nil {
			log.Fatal(err)
		}

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
