package main

import (
	"fmt"
	"github.com/usedatabrew/pglogicalstream"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

func main() {
	var config pglogicalstream.Config
	yamlFile, err := ioutil.ReadFile("./example/simple/config.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	checkPointer, err := NewPgStreamCheckPointer("redis.com:port", "user", "password")
	if err != nil {
		log.Fatalf("Checkpointer error")
	}
	pgStream, err := pglogicalstream.NewPgStream(config, checkPointer)
	if err != nil {
		panic(err)
	}

	pgStream.OnMessage(func(message []byte) {
		fmt.Println(string(message))
	})
}
