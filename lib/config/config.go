package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

const (
	CmdCleanup = 0
)

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	Database int    `yaml:"database"`
}

type AmqpConfig struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Exchange string `yaml:"exchange"`
}

type Config struct {
	Redis RedisConfig `yaml:"redis"`
	Amqp  AmqpConfig  `yaml:"amqp"`
}

func NewConfig(fname string) (*Config, error) {
	cfg, err := LoadConfig(fname)
	if err != nil {
		return nil, fmt.Errorf("NewConfig: %v", err)
	}

	return cfg, nil
}

func LoadConfig(fname string) (*Config, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("LoadConfig ioutil.ReadFile: %v", err)
	}

	// Parse the yaml into a struct
	cfg := &Config{}

	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("LoadConfig yaml.Unmarshal: %v", err)
	}

	return cfg, nil
}
