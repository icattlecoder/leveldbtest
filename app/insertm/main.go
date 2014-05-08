package main 

import (
		"os"
		"fmt"
		"log"
		"labix.org/v2/mgo"
		"time"
		"strconv"
		"flag"
)

type M map[string]interface{}

func main(){
		var limit = flag.Int("limit",1000,"limit result count")
		flag.Parse()
		count := *limit

		session,err:=mgo.Dial("localhost")
		if err!=nil{
				log.Println("mgo.Dial failed",err)
				os.Exit(-1)
		}
		defer session.Close()
		coll:=session.DB("levelTest").C("tbl")

		basekey := "hello.jpg"
		
		fmt.Println("starting insert",count,"items")
		start := time.Now()
		for i:=0;i<count;i++{

			key := basekey + strconv.Itoa(i)

			m:=M{
				"_id":      "7gr4wa:"+key,
				"owner":   	23872243,
				"itbl":     4000000,
				"tbl":      "test",
				"key":      key,
				"fh":       []byte("abcdeffghijjklllmnopqrstuvwxyzxweowd1231212312xawbe"),
				"hash":     "xhaxlakdffwezfljdlakdsfeoidf",
				"fsize":    4000000,
				"mimeType": "image/jpeg",
				"putTime":  time.Now(),
			}
			if err = coll.Insert(m);err != nil {
					log.Println("db.Insert",err,key,m)
			}
		}
		end := time.Now()
		fmt.Println("end insert")
		fmt.Println("Taken",end.Sub(start))
		
}
