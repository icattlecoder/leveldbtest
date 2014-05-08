package main

import(
		"log"
		"fmt"
		"os"
		"github.com/jmhodges/levigo"
		"flag"
		"strconv"
		"time"
		"encoding/json"
)

type M map[string]interface{}

//
func main(){
	var limit = flag.Int("limit",1000,"limit result count")
	flag.Parse()
	count := *limit
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("/tmp/leveldb", opts)
	if err!=nil{
			log.Println("levigo.Open :",err)
			os.Exit(-1)
	}
	log.Println("Open leveldb success!")
	defer db.Close()

	wo := levigo.NewWriteOptions()

	basekey := "hello.jpg"

	fmt.Println("starting insert",count,"items")
	start := time.Now()
	for i:=0;i<count;i++{
		key := basekey + strconv.Itoa(i)
		m:=M{
			"owner":   23872243,
			"itbl":     4000000,
			"tbl":      "test",
			"key":      key,
			"fh":       []byte("abcdeffghijjklllmnopqrstuvwxyzxweowd1231212312xawbe"),
			"hash":     "xhaxlakdffwezfljdlakdsfeoidf",
			"fsize":    4000000,
			"mimeType": "image/jpeg",
			"putTime":  time.Now(),
		}
		bs,_ := json.Marshal(m)

		if err = db.Put(wo,[]byte("7gr4wa:"+key),bs);err!=nil{
				log.Println("db.Put",key,m)
		}
	}

	end := time.Now()
	fmt.Println("end insert")
	fmt.Println("Taken",end.Sub(start))
}

