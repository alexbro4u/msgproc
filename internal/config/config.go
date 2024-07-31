package config

import (
	"github.com/ilyakaznacheev/cleanenv"
	"log"
	"os"
	"time"
)

type Config struct {
	Env string `yaml:"env" env-default:"local"`

	HTTPServer struct {
		Host         string        `yaml:"host"  env-default:"localhost:8080"`
		ReadTimeout  time.Duration `yaml:"read_timeout" env-default:"5s"`
		WriteTimeout time.Duration `yaml:"write_timeout" env-default:"5s"`
		IdleTimeout  time.Duration `yaml:"idle_timeout" env-default:"30s"`
	} `yaml:"http_server"`

	Postgres struct {
		Host     string `yaml:"host" env-default:"localhost"`
		Port     string `yaml:"port" env-default:"5432"`
		Database string `yaml:"database" env-default:"msgproc"`
		User     string `yaml:"user" env-default:"msgproc"`
		Password string `yaml:"password" env-default:"msgproc"`
	} `yaml:"postgres"`

	Kafka struct {
		Hosts string `yaml:"hosts" env-default:"localhost:9092"`
	} `yaml:"kafka"`

	Migrator struct {
		MigrationsPath  string `yaml:"migrations_path" env-default:"./migrations"`
		MigrationsTable string `yaml:"migrations_table" env-required:"true"`
	} `yaml:"migrator"`

	CtxTimeout time.Duration `yaml:"ctx_timeout" env-default:"5s"`
}

func LoadConfig(configPath string, cfg interface{}) {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file not found: %s", configPath)
	}

	if err := cleanenv.ReadConfig(configPath, cfg); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONFIG_PATH is not set")
	}

	var cfg Config
	LoadConfig(configPath, &cfg)
	return &cfg
}
