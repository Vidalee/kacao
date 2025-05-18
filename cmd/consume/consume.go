package consume

import (
	"context"
	"errors"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var consumeCmd = &cobra.Command{
	Use:   "consume <topic_name> [--offset <offset>] [--timeout <seconds>]",
	Short: "Consume messages from a topic",
	Long:  `Consume messages from a topic with an optional timeout.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(command *cobra.Command, args []string) error {
		boostrapServers, err := cmd.GetCurrentClusterBootstrapServers()
		cobra.CheckErr(err)
		consumerGroup, err := cmd.GetConsumerGroup()
		cobra.CheckErr(err)
		offsetArg, err := command.Flags().GetString("offset")
		cobra.CheckErr(err)
		timeoutArg, err := command.Flags().GetString("timeout")
		cobra.CheckErr(err)

		if offsetArg != "" && offsetArg != "earliest" && offsetArg != "latest" {
			return fmt.Errorf("invalid offset argument. Use 'earliest' or 'latest'. By default, the last committed offset by Kacao will be used")
		}

		timeoutDuration := time.Duration(0)
		if timeoutArg != "" {
			seconds, err := strconv.Atoi(timeoutArg)
			if err != nil || seconds < 0 {
				return fmt.Errorf("invalid timeout value. Must be a non-negative integer representing seconds")
			}
			timeoutDuration = time.Duration(seconds) * time.Second
		}

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(boostrapServers...),
			kgo.ConsumerGroup(consumerGroup),
			kgo.ConsumeTopics(args[0]),
		)
		cobra.CheckErr(err)
		adminClient := kadm.NewClient(cl)
		defer cl.Close()
		defer adminClient.Close()

		ctx := context.Background()
		if timeoutDuration > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeoutDuration)
			defer cancel()
		}

		committedListedOffsets, _ := adminClient.ListEndOffsets(ctx, args...)

		if offsetArg != "" {
			var newOffsets kadm.Offsets = make(map[string]map[int32]kadm.Offset)
			newOffsets[args[0]] = make(map[int32]kadm.Offset)
			for _, listedOffset := range committedListedOffsets[args[0]] {
				var offsetValue int64 = 0
				if offsetArg == "latest" {
					offsetValue = listedOffset.Offset
				}
				newOffsets[args[0]][listedOffset.Partition] = kadm.Offset{
					Topic:       listedOffset.Topic,
					Partition:   listedOffset.Partition,
					At:          offsetValue,
					LeaderEpoch: listedOffset.LeaderEpoch,
					Metadata:    "",
				}
			}
			err := adminClient.CommitAllOffsets(ctx, consumerGroup, newOffsets)
			cobra.CheckErr(err)
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("Closing client...")
			cl.Close()
			adminClient.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				_, err := fmt.Fprintln(command.OutOrStdout(), "Timeout reached. Stopping consumer.")
				cobra.CheckErr(err)
				return nil
			default:
				fetches := cl.PollFetches(ctx)
				if errs := fetches.Errors(); len(errs) > 0 {
					if errors.Is(errs[0].Err, context.DeadlineExceeded) {
						return nil
					}
					panic(fmt.Sprint(errs))
				}

				iter := fetches.RecordIter()
				for !iter.Done() {
					record := iter.Next()
					_, err := fmt.Fprintln(command.OutOrStdout(), string(record.Value))
					cobra.CheckErr(err)
				}
			}
		}
	},
}

func init() {
	consumeCmd.Flags().StringP("offset", "o", "", "Offset to start consuming from (latest, earliest, or by default the last committed offset by Kacao)")
	consumeCmd.Flags().StringP("timeout", "t", "", "Timeout in seconds to stop consuming messages")
	cmd.RootCmd.AddCommand(consumeCmd)
}
