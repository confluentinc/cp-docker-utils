package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	brokerMetadataRequestBackoffMs = 1000
	defaultMetadataTimeoutMs       = 5000
)

// parsePropertiesFile reads a Java .properties file and returns a map of key-value pairs.
// It handles comments (lines starting with # or !) and key=value or key:value formats.
func parsePropertiesFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open properties file %q: %w", path, err)
	}
	defer file.Close()

	properties := make(map[string]string)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}

		var key, value string
		if idx := strings.Index(line, "="); idx != -1 {
			key = strings.TrimSpace(line[:idx])
			value = strings.TrimSpace(line[idx+1:])
		} else if idx := strings.Index(line, ":"); idx != -1 {
			key = strings.TrimSpace(line[:idx])
			value = strings.TrimSpace(line[idx+1:])
		} else {
			continue
		}

		if key != "" {
			properties[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading properties file %q: %w", path, err)
	}

	return properties, nil
}

// buildKafkaConfig creates a kafka.ConfigMap from the provided parameters.
// Priority: explicit flags > config file values
func buildKafkaConfig(bootstrapServers, configFile, securityProtocol string) (*kafka.ConfigMap, error) {
	config := &kafka.ConfigMap{}

	if configFile != "" {
		props, err := parsePropertiesFile(configFile)
		if err != nil {
			return nil, err
		}

		for key, value := range props {
			if err := config.SetKey(key, value); err != nil {
				return nil, fmt.Errorf("failed to set config key %q: %w", key, err)
			}
		}
	}

	if bootstrapServers != "" {
		if err := config.SetKey("bootstrap.servers", bootstrapServers); err != nil {
			return nil, fmt.Errorf("failed to set bootstrap.servers: %w", err)
		}
	}

	if securityProtocol != "" {
		if err := config.SetKey("security.protocol", securityProtocol); err != nil {
			return nil, fmt.Errorf("failed to set security.protocol: %w", err)
		}
	}

	bootstrapVal, err := config.Get("bootstrap.servers", "")
	if err != nil || bootstrapVal == "" {
		return nil, fmt.Errorf("bootstrap.servers must be provided via --bootstrap-servers flag or in config file")
	}

	return config, nil
}

// waitForKafkaReady polls the Kafka cluster until the minimum number of brokers are available
// or the timeout expires. Returns nil on success, error on failure.
func waitForKafkaReady(config *kafka.ConfigMap, minBrokers int, timeoutSecs int) error {
	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	timeoutMs := timeoutSecs * 1000
	startTime := time.Now()
	var brokerCount int

	for {
		elapsed := time.Since(startTime)
		remainingMs := timeoutMs - int(elapsed.Milliseconds())

		if remainingMs <= 0 {
			return fmt.Errorf("timeout waiting for kafka: expected %d brokers but found %d", minBrokers, brokerCount)
		}

		metadataTimeout := min(defaultMetadataTimeoutMs, remainingMs)
		metadata, err := adminClient.GetMetadata(nil, true, metadataTimeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting metadata: %v. Retrying...\n", err)
		} else {
			brokerCount = len(metadata.Brokers)
			if brokerCount >= minBrokers {
				fmt.Fprintf(os.Stderr, "Kafka is ready: found %d brokers (expected %d)\n", brokerCount, minBrokers)
				return nil
			}
			fmt.Fprintf(os.Stderr, "Expected %d brokers but found only %d. Retrying...\n", minBrokers, brokerCount)
		}

		sleepDuration := min(brokerMetadataRequestBackoffMs, remainingMs)
		time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	}
}
