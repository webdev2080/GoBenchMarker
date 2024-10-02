package config

import (
	"os"

	"github.com/oracle/oci-go-sdk/v65/common"
)

// LoadOCIConfig loads the OCI configuration from the config file
func LoadOCIConfig() (common.ConfigurationProvider, error) {
	configFile := os.Getenv("OCI_CONFIG_FILE")
	if configFile == "" {
		configFile = "<Config File Path Here>" // Default path for the config file
	}

	// Use OCI SDK to load credentials from the specified file
	provider, err := common.ConfigurationProviderFromFile(configFile, "DEFAULT")
	if err != nil {
		return nil, err
	}

	return provider, nil
}
