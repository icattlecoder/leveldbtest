package main

import(
		"log"
		"fmt"
		"flag"
		"os"
		"github.com/jmhodges/levigo"
		"time"
)

type M map[string]interface{}

//
func main(){
	var key = flag.String("key","","the key start")
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

	log.Println("starting cout",*key)
	start := time.Now()

	it := db.NewIterator(ro)
	defer it.Close()
	it.Seek([]byte(*key))

	i:=0
	for ;it.Valid();it.Next(){
			i+=1
	}

	end := time.Now()

	log.Println("end count")
	log.Println("Taken",end.Sub(start))
	fmt.Println("Count=",i)

}

