package main

import (
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/sbunce/bson"
	bson2 "labix.org/v2/mgo/bson"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type M map[string]interface{}

//
func main() {
	var dbpath = flag.String("dbpath", "data", "")
	var datasrc = flag.String("datasrc", "data", "")
	var onlyKey = flag.Bool("onlyKey", false, "")
	flag.Parse()

	var count int64

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(*dbpath, opts)

	if err != nil {
		log.Println("levigo.Open :", err, *dbpath)
		os.Exit(-1)
	}
	log.Println("Open leveldb success!")
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

	filepath.Walk(*datasrc, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		fi, err2 := os.Open(path)
		if err2 != nil {
			return err2
		}
		for {
			m, err2 := bson.ReadMap(fi)
			if err2 != nil {
				return nil
			}

			// _id, ok := m["_id"].(string)

			_id := fmt.Sprint(m["_id"])

			// log.Println(_id)

			if *onlyKey {
				if err2 = db.Put(wo, []byte(_id), []byte{}); err2 == nil {
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
				bs, err2 := bson2.Marshal(m2)

				if err2 != nil {
					log.Println("xxx")
					continue
				}
				if err2 = db.Put(wo, []byte(_id), bs); err2 == nil {
					atomic.AddInt64(&count, 1)
				}
			}
		}

		return nil

	})

	fmt.Println(count)

	end := time.Now()
	fmt.Println("end insert")
	fmt.Println("Taken", end.Sub(start))
}
