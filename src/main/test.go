package main

import (
	"6.824/mr"
	"bufio"
	"context"
	"encoding/json"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func main() {

	filename := "pg-tom_sawyer.txt"
	nReducer := 10
	jobId := 1

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	intermediate := [][]mr.KeyValue{}
	for i := 0; i < nReducer; i++ {
		t := []mr.KeyValue{}
		intermediate = append(intermediate, t)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	br := bufio.NewReader(file)
	err = os.Mkdir("mapTask"+strconv.Itoa(jobId), os.ModePerm)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			a, _, c := br.ReadLine()
			if c == io.EOF {
				for i := 0; i < nReducer; i++ {
					s := "mapTmp-" + strconv.Itoa(jobId) + "-" + strconv.Itoa(i) + "-"
					tmpfile, err := ioutil.TempFile("./", s)
					defer os.Remove(tmpfile.Name())
					if err != nil {
						log.Println(err)
					}

					enc := json.NewEncoder(tmpfile)
					for _, kv := range intermediate[i] {
						err = enc.Encode(&kv)
						if err != nil {
							log.Println(err)
						}
					}
				}
				goto END
			}

			kva := Map(filename, string(a))
			//fmt.Println(kva)
			var index int
			if len(kva) != 0 {
				for j := 0; j < len(kva); j++ {
					index = ihash(kva[j].Key) % 10
					intermediate[index] = append(intermediate[index], kva...)
				}
			}

		}
	}

END:
	select {
	case <-ctx.Done():
		return
	default:
		tmpFiles, err := filepath.Glob("mapTmp-" + strconv.Itoa(jobId) + "*")
		if err != nil {
			log.Println(err)
		}
		for i := 0; i < len(tmpFiles); i++ {
			str := strings.Split(tmpFiles[i], "-")
			newName := "mr-" + strconv.Itoa(jobId) + "-" + str[2]
			err = os.Rename(tmpFiles[i], "mapTask"+strconv.Itoa(jobId)+"/"+newName)
		}
	}

	return
}
