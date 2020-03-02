package redisclient

import (
	"fmt"

	"github.com/r3boot/suricata-amqp-pipe/lib/config"

	redis "github.com/go-redis/redis"
)

type RedisReader struct {
	Client  *redis.Client
	Control chan int
	Done    chan bool
}

func NewRedisReader(cfg config.RedisConfig) (*RedisReader, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	reader := &RedisReader{
		Control: make(chan int, 1),
	}

	reader.Client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.Password,
		DB:       cfg.Database,
	})

	_, err := reader.Client.Ping().Result()
	if err != nil {
		return nil, fmt.Errorf("NewRedisClient: %v", err)
	}

	return reader, nil
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
				data, err := r.Client.LPop("suricata").Result()
				if err == nil {
					logdata <- []byte(data)
				}
			}
		}
	}

	r.Done <- true
}
