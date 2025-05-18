package test_helpers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/pflag"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

type TestConfig struct {
	Clusters       map[string]map[string]interface{}
	Contexts       map[string]map[string]interface{}
	CurrentContext string
}

func SetupTest(t *testing.T, config TestConfig) string {
	tempDir, err := os.MkdirTemp("", "kacao-test")
	assert.NoError(t, err)

	viper.Reset()
	viper.SetConfigType("yaml")
	viper.AddConfigPath(tempDir)
	viper.SetConfigName(".kacao")

	for clusterName, clusterConfig := range config.Clusters {
		for key, value := range clusterConfig {
			viper.Set("clusters."+clusterName+"."+key, value)
		}
	}
	if config.Clusters != nil && len(config.Clusters) == 0 {
		viper.Set("clusters", map[string]interface{}{})
	}

	for contextName, contextConfig := range config.Contexts {
		for key, value := range contextConfig {
			viper.Set("contexts."+contextName+"."+key, value)
		}
	}
	if config.Contexts != nil && len(config.Contexts) == 0 {
		viper.Set("contexts", map[string]interface{}{})
	}

	err = viper.SafeWriteConfig()
	assert.NoError(t, err)

	return tempDir
}

func CleanupTestConfig(t *testing.T, tempDir string) {
	viper.Reset()
	err := os.RemoveAll(tempDir)
	assert.NoError(t, err)
}

func CleanupKafkaCluster(t *testing.T, client *kgo.Client, adminClient *kadm.Client, ctx context.Context) {

	topicsBeforeCmd, err := adminClient.ListTopics(ctx)
	assert.NoError(t, err)

	var topicNames []string
	for _, topicDetail := range topicsBeforeCmd {
		topicNames = append(topicNames, topicDetail.Topic)
	}
	if len(topicNames) == 0 {
		return
	}

	deleteTopicResponses, err := adminClient.DeleteTopics(ctx, topicNames...)
	assert.NoError(t, err)
	for _, topicResponse := range deleteTopicResponses {
		assert.NoError(t, topicResponse.Err)
	}
	client.PurgeTopicsFromClient(topicNames...)

	// Wait for topic deletion, timeout is 15 seconds (kafka default)
	// I found that 2 seconds is enough for our tests, but if topics still exist
	// when fetching just after cleanup, you might want to wait longer
	time.Sleep(2 * time.Second)
}

func ExecuteCommandWrapper(args []string) (string, error) {
	var buf bytes.Buffer
	ResetSubCommandFlagValues(cmd.RootCmd)

	cmd.RootCmd.SetOut(&buf)
	cmd.RootCmd.SetErr(&buf)
	cmd.RootCmd.SetArgs(args)
	err := cmd.RootCmd.Execute()

	output := buf.String()
	return output, err
}

func ResetSubCommandFlagValues(root *cobra.Command) {
	for _, c := range root.Commands() {
		c.Flags().VisitAll(func(f *pflag.Flag) {
			sliceValueType := reflect.TypeOf((*pflag.SliceValue)(nil)).Elem()
			if reflect.TypeOf(f.Value).Implements(sliceValueType) {
				defValue := strings.Trim(f.DefValue, "[]")
				var defValueParts []string
				// Is the case when empty def value for slices/arrays (f.DefValue = [])
				if defValue != "" {
					defValueParts = strings.Split(defValue, ",")
				}
				sliceValue, _ := f.Value.(pflag.SliceValue)
				_ = sliceValue.Replace(defValueParts)
				return
			}
			_ = f.Value.Set(f.DefValue)
		})
		ResetSubCommandFlagValues(c)
	}
}

func ProduceMessage(t *testing.T, cl *kgo.Client, topic string, value string) {
	t.Helper()

	record := &kgo.Record{
		Topic: topic,
		Value: []byte(value),
	}

	results := cl.ProduceSync(context.Background(), record)
	for _, result := range results {
		assert.NoError(t, result.Err, "Failed to produce message to topic %s", topic)
	}
}

func ProduceMessages(t *testing.T, cl *kgo.Client, topic string, count int) {
	t.Helper()

	records := make([]*kgo.Record, count)
	for i := 0; i < count; i++ {
		records[i] = &kgo.Record{
			Topic: topic,
			Value: []byte(fmt.Sprintf("test message %d", i)),
		}
	}

	results := cl.ProduceSync(context.Background(), records...)
	for _, result := range results {
		assert.NoError(t, result.Err, "Failed to produce message to topic %s", topic)
	}
}

func ProduceMessageWithHeadersAndKey(t *testing.T, cl *kgo.Client, topic string, value string, key string, headers map[string]string) {
	t.Helper()

	record := &kgo.Record{
		Topic:   topic,
		Value:   []byte(value),
		Key:     []byte(key),
		Headers: make([]kgo.RecordHeader, 0, len(headers)),
	}
	for key, val := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   key,
			Value: []byte(val),
		})
	}

	results := cl.ProduceSync(context.Background(), record)
	for _, result := range results {
		assert.NoError(t, result.Err, "Failed to produce message to topic %s", topic)
	}
}

func StringPtr(s string) *string {
	return &s
}
