package amqpclient

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/r3boot/suricata-amqp-pipe/lib/config"
	"github.com/streadway/amqp"
)

type AmqpWriter struct {
	Type       string
	Url        string
	Exchange   string
	Queue      amqp.Queue
	Channel    *amqp.Channel
	Connection *amqp.Connection
	Config     config.AmqpConfig
	inhibit    bool
	Control    chan int
	Done       chan bool
}

func NewAmqpWriter(cfg config.AmqpConfig) (*AmqpWriter, error) {
	user := cfg.Username
	pass := cfg.Password
	host := cfg.Host
	port := strconv.Itoa(cfg.Port)

	url := "amqp://" + user + ":" + pass + "@" + host + ":" + port
	writer := &AmqpWriter{
		Config:   cfg,
		Type:     "suricata",
		Url:      url,
		inhibit:  false,
		Exchange: cfg.Exchange,
		Control:  make(chan int, 1),
		Done:     make(chan bool, 1),
	}

	return writer, nil
}

func (w *AmqpWriter) Connect() error {
	var err error

	w.Connection, err = amqp.Dial(w.Url)
	if err != nil {
		return fmt.Errorf("NewAmqpWriter amqp.Dial: %v", err)
	}

	w.Channel, err = w.Connection.Channel()
	if err != nil {
		return fmt.Errorf("NewAmqpWriter Connection.Channel: %v", err)
	}

	err = w.Channel.ExchangeDeclare(
		w.Config.Exchange, // Name of exchange
		"fanout",          // Type of exchange
		true,              // Durable
		false,             // Auto-deleted
		false,             // Internal queue
		false,             // no-wait
		nil,               // Arguments
	)
	if err != nil {
		return fmt.Errorf("NewAmqpWriter: Channel.ExchangeDeclare: %v", err)
	}

	return nil
}

func (w *AmqpWriter) Setinhibit(newValue bool) {
	w.inhibit = newValue
	if w.inhibit {
		log.Printf("AMQP inhibited, not forwarding any new events\n")
	} else {
		log.Printf("AMQP uninhibited, forwarding events again\n")
	}
}

func (w *AmqpWriter) TryToReconnect() {
	for {
		log.Printf("Trying to reconnect to AMQP\n")

		err := w.Connect()
		if err == nil {
			log.Printf("Reconnected to AMQP\n")
			w.Setinhibit(false)
			break
		}
	}
}

func (w *AmqpWriter) Write(logdata chan []byte) error {
	stop_loop := false
	for {
		if stop_loop {
			break
		}

		select {
		case cmd := <-w.Control:
			{
				switch cmd {
				case config.CmdCleanup:
					{
						stop_loop = true
						continue
					}
				}
			}
		case event := <-logdata:
			{
				if !w.inhibit {
					err := w.Channel.Publish(
						w.Exchange, // exchange to use
						"",         // key to use for routing
						false,      // mandatory
						false,      // immediate
						amqp.Publishing{
							ContentType: "application/json",
							Body:        event,
						},
					)
					if err != nil {
						log.Printf("WARNING: Failed to forward to AMQP\n")
						err = w.Channel.Close()
						if err != nil {
							log.Printf("WARNING: Failed to close AMQP channel\n")
						}
						err = w.Connection.Close()
						if err != nil {
							log.Printf("WARNING: Failed to close AMQP connection\n")
						}
						w.Setinhibit(true)
						go w.TryToReconnect()
					}
				} else {
					time.Sleep(1 * time.Second)
				}
			}
		}
	}

	w.Done <- true

	return nil
}
