package main

import (
	"flag"
	"fmt"
	levigo "github.com/rocksdb"
	"github.com/sbunce/bson"
	"io"
	bson2 "labix.org/v2/mgo/bson"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type M map[string]interface{}

//
func main() {
	var dbpath = flag.String("dbpath", "data", "")
	var datasrc = flag.String("datasrc", "data", "")
	var onlyKey = flag.Bool("onlyKey", false, "")
	var open_files = flag.Int("open_files", 60000, "")
	var block_size = flag.Int("block_size", 65536, "")
	var write_buffer_size = flag.Int("write_buffr_size", 134217728, "default is 128MB")
	var btc = flag.Int("btc", 20, "backend thread count")
	var threads = flag.Int("threads", 8, "threads")

	var proc = flag.Int("proc", 8, "")
	flag.Parse()

	var count int64

	runtime.GOMAXPROCS(*proc)

	env := levigo.NewDefaultEnv()
	env.SetBackgroundThreads(*btc)

	opts := levigo.NewOptions()
	opts.SetWriteBufferSize(*write_buffer_size)
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetEnv(env)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(*open_files)
	opts.SetBlockSize(*block_size)

	fopts := levigo.NewFlushOptions()

	db, err := levigo.Open(*dbpath, opts)

	if err != nil {
		log.Println("levigo.Open :", err, *dbpath)
		os.Exit(-1)
	}
	log.Println("Open rocksdb success!")
	defer db.Close()

	if *datasrc == "" {
		log.Println("datasrc cannot be empty")
		os.Exit(-1)
	}

	go (func() {
		for {
			<-time.After(time.Minute)
			fmt.Println(count)
		}
	})()

	wo := levigo.NewWriteOptions()

	start := time.Now()

	chanFile := make(chan string)

	go (func() {
		filepath.Walk(*datasrc, func(path string, info os.FileInfo, err error) error {

			if info.IsDir() {
				return nil
			}
			chanFile <- path
			return nil
		})
		for {
			chanFile <- ""
		}
	})()

	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go (func() {
			defer wg.Done()

			for {
				filename := <-chanFile
				if filename == "" {
					return
				}

				fi, err := os.Open(filename)
				if err != nil {
					continue
				}
				for {
					m, err := bson.ReadMap(fi)
					if err == io.EOF {
						break
					}
					_id := fmt.Sprint(m["_id"])
					if *onlyKey {
						if err2 := db.Put(wo, []byte(_id), []byte{}); err2 == nil {
							atomic.AddInt64(&count, 1)
						}
					} else {
						m2 := M{
							"hash":     m["hash"],
							"fsize":    m["fsize"],
							"mimeType": m["mimeType"],
							"fh":       m["fh"],
							"putTime":  m["putTime"],
						}
						bs, err := bson2.Marshal(m2)
						if err != nil {
							continue
						}
						if err2 := db.Put(wo, []byte(_id), bs); err2 == nil {
							atomic.AddInt64(&count, 1)
						}
					}
				}
			}
		})()
	}

	wg.Wait()
	fmt.Println("flushing...")
	err = db.Flush(fopts)
	fmt.Println("end flush,err:", err)

	fmt.Println("insert ", count)
	end := time.Now()
	fmt.Println("end insert")
	fmt.Println("Taken", end.Sub(start))
}
