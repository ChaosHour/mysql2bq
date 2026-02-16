package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/pipeline"
	"github.com/spf13/cobra"
)

var configPath string

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start CDC replication",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		cfg, err := config.Load(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		p, err := pipeline.New(cfg)
		if err != nil {
			return fmt.Errorf("failed to create pipeline: %w", err)
		}

		fmt.Println("Starting pipeline...")
		return p.Run(ctx)
	},
}

func init() {
	startCmd.Flags().StringVar(&configPath, "config", "config.yaml", "Path to config file")
	rootCmd.AddCommand(startCmd)
}
