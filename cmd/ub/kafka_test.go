package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParsePropertiesFile(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected map[string]string
		wantErr  bool
	}{
		{
			name: "basic key=value pairs",
			content: `bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT`,
			expected: map[string]string{
				"bootstrap.servers": "localhost:9092",
				"security.protocol": "PLAINTEXT",
			},
		},
		{
			name: "with comments and empty lines",
			content: `# This is a comment
bootstrap.servers=localhost:9092

! Another comment style
security.protocol=SASL_SSL`,
			expected: map[string]string{
				"bootstrap.servers": "localhost:9092",
				"security.protocol": "SASL_SSL",
			},
		},
		{
			name: "colon separator",
			content: `bootstrap.servers:localhost:9092
security.protocol:SSL`,
			expected: map[string]string{
				"bootstrap.servers": "localhost:9092",
				"security.protocol": "SSL",
			},
		},
		{
			name: "with whitespace",
			content: `  bootstrap.servers = localhost:9092
  security.protocol = PLAINTEXT  `,
			expected: map[string]string{
				"bootstrap.servers": "localhost:9092",
				"security.protocol": "PLAINTEXT",
			},
		},
		{
			name:    "value with equals sign",
			content: `some.property=value=with=equals`,
			expected: map[string]string{
				"some.property": "value=with=equals",
			},
		},
		{
			name:     "empty file",
			content:  ``,
			expected: map[string]string{},
		},
		{
			name: "sasl configuration",
			content: `bootstrap.servers=broker1:9092,broker2:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";`,
			expected: map[string]string{
				"bootstrap.servers": "broker1:9092,broker2:9092",
				"security.protocol": "SASL_SSL",
				"sasl.mechanism":    "PLAIN",
				"sasl.jaas.config":  `org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.properties")

			if err := os.WriteFile(tmpFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			got, err := parsePropertiesFile(tmpFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePropertiesFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.expected) {
				t.Errorf("parsePropertiesFile() got %d properties, want %d", len(got), len(tt.expected))
			}

			for k, v := range tt.expected {
				if got[k] != v {
					t.Errorf("parsePropertiesFile() got[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestParsePropertiesFile_FileNotFound(t *testing.T) {
	_, err := parsePropertiesFile("/nonexistent/path/to/file.properties")
	if err == nil {
		t.Error("parsePropertiesFile() expected error for nonexistent file, got nil")
	}
}

func TestBuildKafkaConfig(t *testing.T) {
	tests := []struct {
		name             string
		bootstrapServers string
		configFile       string
		configContent    string
		securityProtocol string
		wantBootstrap    string
		wantSecurity     string
		wantErr          bool
	}{
		{
			name:             "bootstrap servers from flag",
			bootstrapServers: "localhost:9092",
			wantBootstrap:    "localhost:9092",
		},
		{
			name:             "bootstrap servers from flag with security",
			bootstrapServers: "localhost:9092",
			securityProtocol: "SASL_SSL",
			wantBootstrap:    "localhost:9092",
			wantSecurity:     "SASL_SSL",
		},
		{
			name:          "bootstrap servers from config file",
			configContent: "bootstrap.servers=broker1:9092,broker2:9092",
			wantBootstrap: "broker1:9092,broker2:9092",
		},
		{
			name:             "flag overrides config file",
			bootstrapServers: "override:9092",
			configContent:    "bootstrap.servers=original:9092",
			wantBootstrap:    "override:9092",
		},
		{
			name:             "security from flag overrides config",
			bootstrapServers: "localhost:9092",
			configContent:    "security.protocol=PLAINTEXT",
			securityProtocol: "SASL_SSL",
			wantBootstrap:    "localhost:9092",
			wantSecurity:     "SASL_SSL",
		},
		{
			name:    "missing bootstrap servers",
			wantErr: true,
		},
		{
			name:          "empty bootstrap in config",
			configContent: "security.protocol=PLAINTEXT",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var configFilePath string
			if tt.configContent != "" {
				tmpDir := t.TempDir()
				configFilePath = filepath.Join(tmpDir, "kafka.properties")
				if err := os.WriteFile(configFilePath, []byte(tt.configContent), 0644); err != nil {
					t.Fatalf("failed to write config file: %v", err)
				}
			}

			got, err := buildKafkaConfig(tt.bootstrapServers, configFilePath, tt.securityProtocol)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildKafkaConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			bootstrap, _ := got.Get("bootstrap.servers", "")
			if bootstrap != tt.wantBootstrap {
				t.Errorf("buildKafkaConfig() bootstrap.servers = %v, want %v", bootstrap, tt.wantBootstrap)
			}

			if tt.wantSecurity != "" {
				security, _ := got.Get("security.protocol", "")
				if security != tt.wantSecurity {
					t.Errorf("buildKafkaConfig() security.protocol = %v, want %v", security, tt.wantSecurity)
				}
			}
		})
	}
}

func TestBuildKafkaConfig_InvalidConfigFile(t *testing.T) {
	_, err := buildKafkaConfig("", "/nonexistent/config.properties", "")
	if err == nil {
		t.Error("buildKafkaConfig() expected error for nonexistent config file, got nil")
	}
}

func TestCheckKafkaReady_InvalidArgs(t *testing.T) {
	tests := []struct {
		name         string
		minBrokers   string
		timeout      string
		bootstrap    string
		configFile   string
		security     string
		wantErrMatch string
	}{
		{
			name:         "invalid min brokers",
			minBrokers:   "not-a-number",
			timeout:      "10",
			bootstrap:    "localhost:9092",
			wantErrMatch: "invalid min-num-brokers",
		},
		{
			name:         "invalid timeout",
			minBrokers:   "1",
			timeout:      "not-a-number",
			bootstrap:    "localhost:9092",
			wantErrMatch: "invalid timeout",
		},
		{
			name:         "missing bootstrap servers",
			minBrokers:   "1",
			timeout:      "10",
			wantErrMatch: "bootstrap.servers must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkKafkaReady(tt.minBrokers, tt.timeout, tt.bootstrap, tt.configFile, tt.security)
			if err == nil {
				t.Error("checkKafkaReady() expected error, got nil")
				return
			}
			if tt.wantErrMatch != "" && !contains(err.Error(), tt.wantErrMatch) {
				t.Errorf("checkKafkaReady() error = %v, want match %q", err, tt.wantErrMatch)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
