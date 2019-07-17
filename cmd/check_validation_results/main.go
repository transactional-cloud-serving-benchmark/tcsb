package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
)

func main() {
	if !(len(os.Args) >= 3) {
		log.Fatalf("Usage: %s <filename> <filename> [additional filenames...]", os.Args[0])
	}

	// Open the files and create line scanners for each of them:
	scanners := []*bufio.Scanner{}
	for _, filename := range os.Args[1:] {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		s := bufio.NewScanner(f)
		scanners = append(scanners, s)
	}

	// For each scanner, advance its progression through its line-oriented
	// input, checking that the scanners match up with each other.
	statuses := make([]bool, 0, len(scanners))
	texts := make([][]byte, 0, len(scanners))
	mismatches, i := 0, 0
	for {
		statuses = statuses[:0]
		texts = texts[:0]

		for _, s := range scanners {
			status := s.Scan()
			statuses = append(statuses, status)
		}

		seenFalse := false
		seenTrue := false
		for _, status := range statuses {
			seenFalse = seenFalse || status == false
			seenTrue = seenTrue || status == true
		}

		if seenFalse && seenTrue {
			log.Printf("some scanners terminated while others did not. line %d: %v", i, statuses)
			break
		}

		if seenFalse {
			// All done.
			break
		}

		for _, s := range scanners {
			texts = append(texts, s.Bytes())
		}

		for _, text := range texts[1:] {
			if !bytes.Equal(text, texts[0]) {
				log.Printf("mismatched input on line %d: %v", i, texts)
				mismatches++
			}
		}

		i++
	}

	if ratio := float64(mismatches) / float64(i); ratio >= 0.1 {
		log.Fatalf("mismatch ratio too high: %f", ratio)
	}

	log.Printf("validation completed successfully. %d lines.", i)
}
