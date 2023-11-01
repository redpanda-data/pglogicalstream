package main

import (
	"fmt"
	"github.com/usedatabrew/pglogicalstream"
	"github.com/usedatabrew/pglogicalstream/internal/replication"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
)

func main() {
	rules := []tracer.SamplingRule{tracer.RateRule(1)}
	tracer.Start(
		tracer.WithSamplingRules(rules),
		tracer.WithService("pglogicalstream"),
		tracer.WithEnv("local"),
	)
	defer tracer.Stop()

	err := profiler.Start(
		profiler.WithService("pglogicalstream"),
		profiler.WithEnv("local"),
		profiler.WithProfileTypes(
			profiler.CPUProfile,
			profiler.HeapProfile,

			// The profiles below are disabled by
			// default to keep overhead low, but
			// can be enabled as needed.
			profiler.BlockProfile,
			profiler.MutexProfile,
			profiler.GoroutineProfile,
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer profiler.Stop()

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

	pgStream.OnMessage(func(message replication.Wal2JsonChanges) {
		fmt.Println(message.Changes)
	})
}
