package config

import (
	"encoding/json"
	"time"
	"transfers-api/internal/logging"

	"github.com/caarlos0/env/v10"
)

type Config struct {
	Business      BusinessConfig `json:"business"`
	MongoDBConfig MongoDB        `json:"mongodb"`
	MySqlDBConfig MySqlDB        `json:"mysqldb"`
}

type BusinessConfig struct {
	TransferMinAmount int `env:"TRANSFER_MIN_AMOUNT" envDefault:"1" json:"transfer_min_amount"`
}

type MongoDB struct {
	ConnectTimeout time.Duration `env:"MONGODB_CONNECT_TIMEOUT" envDefault:"10s" json:"connect_timeout"`
	Hostname       string        `env:"MONGODB_HOSTNAME" envDefault:"mongodb" json:"hostname"`
	Port           int           `env:"MONGODB_PORT" envDefault:"27017" json:"port"`
	Username       string        `env:"MONGODB_USERNAME" envDefault:"root" json:"username"`
	Password       string        `env:"MONGODB_PASSWORD" envDefault:"root" json:"password"`
	Database       string        `env:"MONGODB_DATABASE" envDefault:"transfers-db" json:"database"`
	Collection     string        `env:"MONGODB_COLLECTION" envDefault:"transfers" json:"collection"`
}
type MySqlDB struct {
	ConnectTimeout time.Duration `env:"MONGODB_CONNECT_TIMEOUT" envDefault:"10s" json:"connect_timeout"`
	Hostname       string        `env:"MONGODB_HOSTNAME" envDefault:"mongodb" json:"hostname"`
	Port           int           `env:"MONGODB_PORT" envDefault:"27017" json:"port"`
	Username       string        `env:"MONGODB_USERNAME" envDefault:"root" json:"username"`
	Password       string        `env:"MONGODB_PASSWORD" envDefault:"root" json:"password"`
	Database       string        `env:"MONGODB_DATABASE" envDefault:"transfers-db" json:"database"`
	Collection     string        `env:"MONGODB_COLLECTION" envDefault:"transfers" json:"collection"`
}

func ParseFromEnv() *Config {
	var cfg Config
	for _, nested := range []interface{}{
		&cfg.Business,
		&cfg.MongoDBConfig,
	} {
		if err := env.Parse(nested); err != nil {
			logging.Logger.Fatalf("error parsing config: %v", err)
		}
	}
	return &cfg
}

func ParseFromJSON(input []byte) *Config {
	var cfg Config
	if err := json.Unmarshal(input, &cfg); err != nil {
		logging.Logger.Fatalf("error parsing config: %v", err)
	}
	return &cfg
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		logging.Logger.Fatalf("error marshaling config: %v", err)
	}
	return string(bytes)
}
