package redisclient

import (
	"fmt"
	"log"
	"time"

	"github.com/r3boot/suricata-amqp-pipe/lib/config"

	redis "github.com/go-redis/redis"
)

type RedisReader struct {
	Client  *redis.Client
	Config  config.RedisConfig
	inhibit bool
	Control chan int
	Done    chan bool
}

func NewRedisReader(cfg config.RedisConfig) (*RedisReader, error) {

	reader := &RedisReader{
		Config:  cfg,
		inhibit: false,
		Control: make(chan int, 1),
		Done:    make(chan bool, 1),
	}

	err := reader.Connect()
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (r *RedisReader) Connect() error {
	addr := fmt.Sprintf("%s:%d", r.Config.Host, r.Config.Port)
	r.Client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: r.Config.Password,
		DB:       r.Config.Database,
	})

	_, err := r.Client.Ping().Result()
	if err != nil {
		return fmt.Errorf("RedisClient.Connect: %v", err)
	}

	return nil
}

func (r *RedisReader) Setinhibit(newValue bool) {
	r.inhibit = newValue
	if r.inhibit {
		log.Printf("Redis inhibited, not reading any new events\n")
	} else {
		log.Printf("Redis uninhibited, reading events again\n")
	}
}

func (r *RedisReader) TryToReconnect() {
	for {
		log.Printf("Trying to reconnect to redis")

		err := r.Connect()
		if err == nil {
			log.Printf("Reconnected to redis")
			r.Setinhibit(false)
			break
		}

		time.Sleep(5 * time.Second)
	}
}

func (r *RedisReader) Read(logdata chan []byte) {
	stopLoop := false
	for {
		if stopLoop {
			break
		}

		select {
		case cmd := <-r.Control:
			{
				switch cmd {
				case config.CmdCleanup:
					{
						stopLoop = true
						continue
					}
				}
			}
		default:
			{
				if !r.inhibit {
					data, err := r.Client.LPop("suricata").Result()
					if err == nil {
						logdata <- []byte(data)
					} else {
						_, err = r.Client.Ping().Result()
						if err != nil {
							log.Printf("WARNING: Redis did not respond to ping\n")
							err = r.Client.Close()
							if err != nil {
								log.Printf("WARNING: Failed to close connection to redis\n")
							}
							r.Client = nil
							r.Setinhibit(true)
							go r.TryToReconnect()
						}
						time.Sleep(1 * time.Second)
					}
				} else {
					time.Sleep(1 * time.Second)
				}
			}
		}
	}

	r.Done <- true
}
