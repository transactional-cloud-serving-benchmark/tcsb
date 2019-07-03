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
	stderr *bufio.Reader
}

func NewIPCDriver(stdin io.WriteCloser, stdout, stderr io.ReadCloser) IPCDriver {
	return IPCDriver{
		stdin:  stdin,
		stdout: stdout,
		stderr: bufio.NewReader(stderr),
	}
}

func RunIPCDriver(ipcDriver IPCDriver, nValidation uint64, validationFilename string, nBurnIn uint64) {
	stdin := bufio.NewReader(os.Stdin)
	//stderr := bufio.NewWriter(os.Stderr)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		_, err := io.Copy(ipcDriver.stdin, stdin)
		ipcDriver.stdin.Close()
		if err == io.ErrClosedPipe {
			// not a problem
		} else if err != nil {
			log.Fatalf("0 ", err)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for {
			data, err := ipcDriver.stderr.ReadBytes('\n')
			log.Printf("client log: %s", data)
			if err == io.ErrClosedPipe || err == io.EOF {
				break
				// not a problem
			} else if err != nil {
				log.Fatalf("2 ", err)
			}
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
			log.Fatalf("bad decoding: ", err)
			break
		}

		collector.Update(reply)
	}
	wg.Wait()
	collector.Finish()
}
