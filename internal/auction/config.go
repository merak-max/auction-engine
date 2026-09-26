package auction

import (
	"encoding/json"
	"errors"
	"io"
	"os"
)

// LoadConfig rejects unknown fields and trailing data so typos cannot silently
// change a budget policy. New performs semantic validation.
func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	var cfg Config
	if err := d.Decode(&cfg); err != nil {
		return Config{}, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Config{}, errors.New("unexpected content after config")
	}
	return cfg, nil
}
