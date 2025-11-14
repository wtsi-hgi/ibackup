package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/wtsi-hgi/ibackup/transformer"
)

type tx struct {
	Description string
	Re, Replace string
}

var Config struct {
	Transformers map[string]tx `json:"transformers"`
}

func newConfig(config string) error {
	f, err := os.Open(config)
	if err != nil {
		return fmt.Errorf("error opening ibackup config: %w", err)
	}

	if err = json.NewDecoder(f).Decode(&Config); err != nil {
		return fmt.Errorf("error parsing ibackup config: %w", err)
	}

	return nil
}

func LoadConfig(path string) error {
	if err := newConfig(path); err != nil {
		return err
	}

	for name, tx := range Config.Transformers {
		err := transformer.Register(name, tx.Re, tx.Replace)
		if err != nil {
			return fmt.Errorf("error registering transformer (%s): %w", name, err)
		}
	}

	return nil
}
