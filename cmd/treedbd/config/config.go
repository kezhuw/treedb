package config

import (
	"io/ioutil"
	"time"

	"github.com/kezhuw/toml"
)

type Config struct {
	Listens            []string `toml:"listens"`
	DataDir            string   `toml:"data-dir"`
	DataReadonly       bool     `toml:"data-readonly"`
	ShutdownTimeout    Duration `toml:"shutdown-timeout"`
	ConcurrentRequests int      `toml:"concurrent-requests"`
}

type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	t, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(t)
	return nil
}

func (d *Duration) MarshalText() ([]byte, error) {
	s := time.Duration(*d).String()
	return []byte(s), nil
}

func Load(cfg *Config, filename string) error {
	text, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return toml.Unmarshal(text, cfg)
}
