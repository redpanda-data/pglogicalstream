package main

import (
	"fmt"
	"github.com/usedatabrew/pglogicalstream"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"strings"
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

	pgStream, err := pglogicalstream.NewPgStream(config, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println(strings.Join([]string{"rides", "test"}, ","))

	pgStream.OnMessage(func(message []byte) {
		fmt.Println(string(message))
	})
}
