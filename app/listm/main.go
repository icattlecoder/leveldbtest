package main 

import (
		"os"
		"fmt"
		"log"
		"labix.org/v2/mgo"
		"time"
		"flag"
)

type M map[string]interface{}

func main(){
		var key = flag.String("key","","the key start")
		var limit = flag.Int("limit",1000,"limit result count")
		flag.Parse()
		session,err:=mgo.Dial("localhost")
		if err!=nil{
				log.Println("mgo.Dial failed",err)
				os.Exit(-1)
		}
		defer session.Close()
		coll:=session.DB("levelTest").C("tbl")

		
		log.Println("starting list",*key,"limit",*limit)
		start := time.Now()
		var items []M
		cond := M{"$regex":"^"+*key}
		if *key != ""{
				cond["$gt"] = *key
		}

		err = coll.Find(M{"_id":cond}).Sort("_id").Limit(*limit).All(&items)

		end := time.Now()
		if err!=nil{
				log.Println("list failed",err)
		}

		for _,v :=range items{
				fmt.Println(v["_id"])
		}


		log.Println("end list")
		log.Println("Taken",end.Sub(start))
}
