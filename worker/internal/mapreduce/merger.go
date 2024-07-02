package mapreduce

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
)

func Merge(files []string, outputFile string) {
	wordCount := make(map[string]int)
	for _, file := range files {
		log.Printf("Reading: %s", file)
		readFile, err := os.Open(file)
		defer func() {
			err := readFile.Close()
			if nil != err {
				log.Fatal(err)
			}
		}()
		if nil != err {
			log.Fatal(err)
		}
		fileScanner := bufio.NewScanner(readFile)
		fileScanner.Split(bufio.ScanLines)

		for fileScanner.Scan() {
			fileLine := fileScanner.Text()
			words := strings.Split(fileLine, " ")
			count, err := strconv.Atoi(words[1])
			if nil != err {
				log.Fatal(err)
			}
			wordCount[words[0]] += count
		}
	}

	file, err := os.Create(outputFile)
	if nil != err {
		log.Fatal(err)
	}
	defer file.Close()

	b, err := json.Marshal(wordCount)
	if nil != err {
		log.Fatal(err)
	}

	err = os.WriteFile(outputFile, b, 0644)
	if nil != err {
		log.Fatal(err)
	}
}
