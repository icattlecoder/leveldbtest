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

			if err = coll.Update(M{"_id":"7gr4wa:"+key},M{"$set":M{"mimeType":"image/png"}});err != nil{
					log.Println("db.Insert",err,key)
			}
		}
		end := time.Now()
		fmt.Println("end insert")
		fmt.Println("Taken",end.Sub(start))
}
