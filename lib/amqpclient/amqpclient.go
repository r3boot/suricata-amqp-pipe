package amqpclient

import (
	"fmt"
	"log"
	"strconv"

	"github.com/r3boot/suricata-amqp-pipe/lib/config"
	"github.com/streadway/amqp"
)

type AmqpWriter struct {
	Name       string
	Type       string
	Url        string
	Exchange   string
	Queue      amqp.Queue
	Channel    *amqp.Channel
	Connection *amqp.Connection
	Control    chan int
	Done       chan bool
}

func NewAmqpWriter(cfg config.AmqpConfig) (*AmqpWriter, error) {
	var err error

	user := cfg.Username
	pass := cfg.Password
	host := cfg.Host
	port := strconv.Itoa(cfg.Port)

	url := "amqp://" + user + ":" + pass + "@" + host + ":" + port
	as := &AmqpWriter{
		Name:     cfg.Name,
		Type:     "suricata",
		Url:      url,
		Exchange: cfg.Exchange,
		Control:  make(chan int, 1),
		Done:     make(chan bool, 1),
	}

	as.Connection, err = amqp.Dial(as.Url)
	if err != nil {
		return nil, fmt.Errorf("NewAmqpWriter amqp.Dial: %v", err)
	}

	as.Channel, err = as.Connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("NewAmqpWriter Connection.Channel: %v", err)
	}

	err = as.Channel.ExchangeDeclare(
		cfg.Exchange, // Name of exchange
		"fanout",     // Type of exchange
		true,         // Durable
		false,        // Auto-deleted
		false,        // Internal queue
		false,        // no-wait
		nil,          // Arguments
	)
	if err != nil {
		return nil, fmt.Errorf("NewAmqpWriter: Channel.ExchangeDeclare: %v", err)
	}

	return as, nil
}

func (as *AmqpWriter) Write(logdata chan []byte) error {
	defer as.Channel.Close()
	defer as.Connection.Close()

	stop_loop := false
	for {
		if stop_loop {
			break
		}

		select {
		case event := <-logdata:
			{
				err := as.Channel.Publish(
					as.Exchange, // exchange to use
					"",          // key to use for routing
					false,       // mandatory
					false,       // immediate
					amqp.Publishing{
						ContentType: "application/json",
						Body:        event,
					},
				)
				if err != nil {
					log.Printf("WARNING: AmqpWriter.Ship Channel.Publish: %v", err)
					continue
				}
			}
		case cmd := <-as.Control:
			{
				switch cmd {
				case config.CmdCleanup:
					{
						stop_loop = true
						continue
					}
				}
			}
		}
	}

	as.Done <- true

	return nil
}
