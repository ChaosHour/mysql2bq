package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

var statusHost string
var statusPort int

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check the status of a running mysql2bq instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Try to connect to the metrics endpoint
		url := fmt.Sprintf("http://%s:%d/metrics", statusHost, statusPort)

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(url)
		if err != nil {
			return fmt.Errorf("failed to connect to mysql2bq instance at %s:%d: %w\nMake sure mysql2bq is running with HTTP metrics enabled", statusHost, statusPort, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
		}

		var metrics map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
			return fmt.Errorf("failed to decode metrics response: %w", err)
		}

		// Pretty print the metrics
		fmt.Println("mysql2bq Status:")
		fmt.Println("================")

		if uptime, ok := metrics["uptime"].(string); ok {
			fmt.Printf("Uptime: %s\n", uptime)
		}

		if totalEvents, ok := metrics["total_events_processed"].(float64); ok {
			fmt.Printf("Total Events Processed: %.0f\n", totalEvents)
		}

		if totalRows, ok := metrics["total_rows_processed"].(float64); ok {
			fmt.Printf("Total Rows Processed: %.0f\n", totalRows)
		}

		if eventsPerSec, ok := metrics["events_per_second"].(float64); ok {
			fmt.Printf("Events/Second: %.2f\n", eventsPerSec)
		}

		if rowsPerSec, ok := metrics["rows_per_second"].(float64); ok {
			fmt.Printf("Rows/Second: %.2f\n", rowsPerSec)
		}

		if activeTx, ok := metrics["active_transactions"].(float64); ok {
			fmt.Printf("Active Transactions: %.0f\n", activeTx)
		}

		if bufferSize, ok := metrics["transaction_buffer_size"].(float64); ok {
			fmt.Printf("Transaction Buffer Size: %.0f\n", bufferSize)
		}

		if batchesWritten, ok := metrics["total_batches_written"].(float64); ok {
			fmt.Printf("Total Batches Written: %.0f\n", batchesWritten)
		}

		if writeErrors, ok := metrics["total_write_errors"].(float64); ok {
			fmt.Printf("Total Write Errors: %.0f\n", writeErrors)
		}

		if currentPos, ok := metrics["current_position"].(string); ok && currentPos != "" {
			fmt.Printf("Current Position: %s\n", currentPos)
		}

		if lastError, ok := metrics["last_error"].(string); ok && lastError != "" {
			fmt.Printf("Last Error: %s\n", lastError)
		}

		fmt.Println("\nHTTP Metrics Endpoint: " + url)

		return nil
	},
}

func init() {
	statusCmd.Flags().StringVar(&statusHost, "host", "localhost", "Host where mysql2bq is running")
	statusCmd.Flags().IntVar(&statusPort, "port", 8080, "Port where mysql2bq metrics are exposed")
	rootCmd.AddCommand(statusCmd)
}
