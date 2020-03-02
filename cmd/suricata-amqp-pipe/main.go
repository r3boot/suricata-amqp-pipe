package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/r3boot/suricata-amqp-pipe/lib/amqpclient"

	"github.com/r3boot/suricata-amqp-pipe/lib/config"
	"github.com/r3boot/suricata-amqp-pipe/lib/redisclient"
)

const (
	defConfigFileValue = "suricata-amqp-pipe.yml"
)

var (
	reader *redisclient.RedisReader
	writer *amqpclient.AmqpWriter
)

func signalHandler(signals chan os.Signal, done chan bool) {
	for _ = range signals {
		reader.Control <- config.CmdCleanup
		<-reader.Done
		writer.Control <- config.CmdCleanup
		<-writer.Done
		done <- true
	}
}

func main() {
	var (
		cfgfile = flag.String("f", defConfigFileValue, "Configuration file to use")
		cfg     = &config.Config{}
	)
	flag.Parse()

	log.SetOutput(os.Stdout)

	signals := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)

	signal.Notify(signals, os.Interrupt, os.Kill)
	go signalHandler(signals, cleanupDone)

	if *cfgfile == "" {
		log.Fatalf("ERROR: config file cannot be nil\n")
	}

	cfg, err := config.NewConfig(*cfgfile)
	if err != nil {
		log.Fatalf("ERROR: %v\n", err)
	}

	reader, err := redisclient.NewRedisReader(cfg.Redis)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Printf("Connected to %s:%d\n", cfg.Redis.Host, cfg.Redis.Port)

	writer, err = amqpclient.NewAmqpWriter(cfg.Amqp)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Printf("Connected to %s:%d", cfg.Amqp.Host, cfg.Amqp.Port)

	logdata := make(chan []byte)
	go reader.Read(logdata)
	go writer.Write(logdata)

	<-cleanupDone
}
