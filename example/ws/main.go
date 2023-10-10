package main

import (
	"github.com/gorilla/websocket"
	"github.com/usedatabrew/pglogicalstream"
	"gopkg.in/yaml.v3"
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

	pgStream, err := pglogicalstream.NewPgStream(config, nil)
	if err != nil {
		panic(err)
	}

	wsClient, _, err := websocket.DefaultDialer.Dial("ws://localhost:10000/ws", nil)
	if err != nil {
		panic(err)
	}
	defer wsClient.Close()

	pgStream.OnMessage(func(message []byte) {
		err := wsClient.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Println("write:", err)
			return
		}
	})
}
