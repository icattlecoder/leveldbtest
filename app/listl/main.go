package main

import(
		"log"
		"fmt"
		"flag"
		"os"
		"github.com/jmhodges/levigo"
		"time"
		"encoding/json"
)

type M map[string]interface{}

//
func main(){
	var key = flag.String("key","","the key start")
	var limit = flag.Int("limit",1000,"limit result count")
	flag.Parse()
	//count := 10000*10
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

	ro := levigo.NewReadOptions()

	//basekey := "hello.jpg"

	log.Println("starting list",*key,"limit",*limit)
	start := time.Now()
	
	it := db.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte(*key))

	keys:=make([]string,*limit)
	vals := make([]M,*limit)

	for i:=0;it.Valid()&&i<*limit;it.Next(){
			keys[i] = string(it.Key())
			m:=M{}
			json.Unmarshal(it.Value(),&m)
			vals[i] = m
			i+=1
	}


	end := time.Now()
	for _,v :=range keys{
			fmt.Println(v)
	}

	log.Println("end list")
	log.Println("Taken",end.Sub(start))

}

