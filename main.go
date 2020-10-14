package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type entry struct {
	name      string
	wallclock float64
	cores     float64
	wg        *sync.WaitGroup
}

type result struct {
	user      string
	corehours float64
}

func searchFile(path string, term string) result {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	entriesC := make(chan entry)
	wg := sync.WaitGroup{}
	result := result{user: term, corehours: 0.0}

	go func() {
		for {
			select {
			case entry, ok := <-entriesC:
				if ok {
					result.corehours = result.corehours + ((entry.wallclock * entry.cores) / (3600.0))
					entry.wg.Done()
				}
			}
		}
	}()

	linesChunkLen := 64 * 1024
	lines := make([]string, 0, 0)
	scanner.Scan()
	for {
		willScan := scanner.Scan()
		lines = append(lines, scanner.Text())
		if len(lines) == linesChunkLen || !willScan {
			toProcess := lines
			wg.Add(len(toProcess))
			process := lines
			go func() {
				for _, text := range process {
					e := entry{wg: &wg}
					acctEntry := strings.Split(text, ":")
					if len(acctEntry) > 1 {
						if acctEntry[3] == term {
							e.name = acctEntry[3]
							e.wallclock, _ = strconv.ParseFloat(acctEntry[13], 64)
							e.cores, _ = strconv.ParseFloat(acctEntry[34], 64)
							entriesC <- e
						} else {
							e.wg.Done()
						}
					} else {
						e.wg.Done()
					}
				}
			}()
			lines = make([]string, 0, linesChunkLen)
		}
		if !willScan {
			break
		}
	}
	wg.Wait()
	close(entriesC)
	return result
}

func main() {
	path := os.Args[1]
	term := os.Args[2]
	result := searchFile(path, term)
	fmt.Println("User: ", result.user)
	fmt.Println("Corehours: ", result.corehours)
}
