package main

import (
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/pglogicalstream"
	"github.com/usedatabrew/pglogicalstream/messages"
	"gopkg.in/yaml.v3"
	"io/ioutil"
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

	pgStream, err := pglogicalstream.NewPgStream(config, log.WithPrefix("pg-cdc"))
	if err != nil {
		panic(err)
	}

	pgStream.OnMessage(func(message messages.Wal2JsonChanges) {
		fmt.Println(message.Changes)
	})
}
