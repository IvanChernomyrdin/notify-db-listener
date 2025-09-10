package config

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Kafka  KafkaConfig  `yaml:"kafka" required:"true"`
	Db     DbConfig     `yaml:"db" required:"true"`
	Logger LoggerConfig `yaml:"logger" required:"true"`
}

type KafkaConfig struct {
	Host    string   `yaml:"host" required:"true"`
	Port    int      `yaml:"port" required:"true"`
	Topic   string   `yaml:"topic" required:"true"`
	GroupID string   `yaml:"groupID" required:"true"`
	Brokers []string `yaml:"brokers" required:"true"`
}

type DbConfig struct {
	Driver         string   `yaml:"driver" required:"true"`
	Host           string   `yaml:"host" required:"true"`
	Port           int      `yaml:"port" required:"true"`
	User           string   `yaml:"user" required:"true"`
	Password       string   `yaml:"password" required:"true"`
	Name           string   `yaml:"name" required:"true"`
	TableEmail     string   `yaml:"table_email" required:"true"`
	NotifyChannels []string `yaml:"notify_channels" required:"true"`
	SslMode        string   `yaml:"ssl_mode" default:"disable"`
}

type LoggerConfig struct {
	GRPCAddress  string `yaml:"grpc_address" required:"true"`
	FallbackPath string `yaml:"fallback_path" required:"true"`
	ServiceName  string `yaml:"service_name" required:"true"`
}

func MustLoad() *Config {
	configPath := fetchConfigPath()
	if configPath == "" {
		panic("config path is empty")
	}

	return MustLoadPath(configPath)
}

func MustLoadPath(configPath string) *Config {
	// check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic("config file does not exist: " + configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("cannot read config: " + err.Error())
	}

	return &cfg
}

// fetchConfigPath fetches config path from command line flag or environment variable.
// Priority: flag > env > default.
// Default value is empty string.
func fetchConfigPath() string {
	var res string

	flag.StringVar(&res, "config", "", "path to config file")
	flag.Parse()

	if res == "" {
		res = os.Getenv("CONFIG_PATH")
	}

	return res
}

func (c DbConfig) RedactedDSN() string {
	dsn, err := c.DSN()
	if err != nil {
		return "<invalid dsn: " + err.Error() + ">"
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid dsn>"
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "REDACTED")
	}
	return u.String()
}

func (c *DbConfig) DSN() (string, error) {
	switch c.Driver {
	case "postgres":
		return c.postgresDSN()
	default:
		return "", fmt.Errorf("unsupported driver: %s", c.Driver)
	}
}

// PostgreSQL URI формируем через net/url — это безопаснее, чем fmt.Sprintf с ручным экранированием.
func (c DbConfig) postgresDSN() (string, error) {
	if err := c.validateBase(); err != nil {
		return "", err
	}
	hostPort := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password), // безопасно экранирует спецсимволы
		Host:   hostPort,
		Path:   "/" + c.Name, // ведущий '/' обязателен
	}
	q := u.Query()
	if c.SslMode != "" {
		q.Set("sslmode", c.SslMode)
	} else {
		q.Set("sslmode", "disable")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c DbConfig) validateBase() error {
	if c.Host == "" {
		return errors.New("db.host is empty")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("db.port is invalid: %d", c.Port)
	}
	if c.User == "" {
		return errors.New("db.user is empty")
	}
	if c.Name == "" {
		return errors.New("db.name is empty")
	}
	return nil
}
