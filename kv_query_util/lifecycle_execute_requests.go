package kv_query_util

import (
	"bufio"
	"io"
	"log"
	"os"
	"sync"

	"github.com/transactional-cloud-serving-benchmark/tcsb/serialization_util"
)

type IPCDriver struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func NewIPCDriver(stdin io.WriteCloser, stdout, stderr io.ReadCloser) IPCDriver {
	return IPCDriver{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

func RunIPCDriver(ipcDriver IPCDriver, nValidation uint64, validationFilename string, nBurnIn uint64) {
	ipcDriver.Setup()
	defer ipcDriver.Teardown()

	stdin := bufio.NewReader(os.Stdin)
	stderr := bufio.NewWriter(os.Stderr)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		_, err := io.Copy(ipcDriver.stdin, stdin)
		ipcDriver.stdin.Close()
		if err == io.ErrClosedPipe {
			// not a problem
		} else if err != nil {
			log.Fatal("0 ", err)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		_, err := io.Copy(stderr, ipcDriver.stderr)
		if err == io.ErrClosedPipe {
			// not a problem
		} else if err != nil {
			log.Fatal("2 ", err)
		}
		wg.Done()
	}()

	collector := NewReplyCollector(nBurnIn, nValidation, validationFilename)
	buf := make([]byte, 0, 4096)
	for {

		reply, err := serialization_util.DecodeNextReply(ipcDriver.stdout, &buf)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("bad decoding: ", err)
		}

		collector.Update(reply)
	}
	wg.Wait()
	collector.Finish()
}
