package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"kafka-log-parser/internal/parser"
	"kafka-log-parser/internal/utils"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	freID string

	rootCmd = &cobra.Command{
		Use:   "kafka-log-parser",
		Short: "Extract and filter Kafka log data",
		Long: `This tool extracts and filters data from Kafka log files. 
You can either specify a single log file or search for log files 
within a directory based on a topic suffix. The extracted JSON 
data can be further filtered by a specific FRE ID.`,
	}

	// Define the command for searching within a directory
	searchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search for log files in a directory and extract data",
		Run: func(cmd *cobra.Command, args []string) {
			dir, _ := cmd.Flags().GetString("dir")     // Get the value of the "dir" flag
			topic, _ := cmd.Flags().GetString("topic") // Get the value of the "topic" flag

			FilterIdsOnly, _ := cmd.Flags().GetBool("filterId")
			IdCompleteMatch, _ := cmd.Flags().GetBool("idMatch")

			if dir == "" || topic == "" {
				fmt.Fprintln(os.Stderr, "Error: Both -dir and -topic flags are required for the 'search' command")
				os.Exit(1)
			}

			inputParams := parser.DefaultInputParams()
			inputParams.FilterIdsOnly = FilterIdsOnly
			inputParams.IdCompleteMatch = IdCompleteMatch

			logFileExtension := ".log"
			logFiles, err := utils.SearchFiles(dir, topic, logFileExtension)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error searching files: %v\n", err)
				os.Exit(1)
			}

			if len(logFiles) == 0 {
				fmt.Fprintf(os.Stderr, "No log files found with topic: %s\n", topic)
				os.Exit(1)
			} else {
				fmt.Printf("Log files with topic %s: %d\n", topic, len(logFiles))
				kafkaEvents := parser.BulkExecute(logFiles, freID, inputParams)
				if len(kafkaEvents) > 0 {
					// Convert to JSON with indentation for readability
					jsonData, err := json.MarshalIndent(kafkaEvents, "", "  ")
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
						os.Exit(1)
					}

					// Write to records.json file
					err = os.WriteFile("records.json", jsonData, 0644)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error writing to records.json: %v\n", err)
						os.Exit(1)
					}

					fmt.Printf("Successfully saved %d records to records.json\n", len(kafkaEvents))
				} else {
					fmt.Println("No matching Kafka events found. No file created.")
				}
			}
		},
	}

	// Define the command for processing a single file
	parseCmd = &cobra.Command{
		Use:   "parse",
		Short: "Parse a single log file and extract data",
		Run: func(cmd *cobra.Command, args []string) {
			file, _ := cmd.Flags().GetString("file") // Get the value of the "file" flag

			if file == "" {
				fmt.Fprintln(os.Stderr, "Error: -file flag is required for the 'parse' command")
				os.Exit(1)
			}

			inputParams := parser.DefaultInputParams()

			kafkaEvents := parser.Execute(file, freID, inputParams)
			if len(kafkaEvents) > 0 {
				for _, data := range kafkaEvents {
					fmt.Println(data)
				}
			} else {
				fmt.Println("No matching Kafka events found.")
			}
		},
	}
)

// Execute is the entry point for the Cobra command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Persistent flag for FRE ID
	rootCmd.PersistentFlags().StringVarP(&freID, "freid", "i", "", "FRE ID to filter JSON data (required)")
	_ = rootCmd.MarkPersistentFlagRequired("freid") // Mark freid as required

	// Add flags to the search command
	searchCmd.Flags().StringP("dir", "d", "", "Root directory to search for log files (required)")
	_ = searchCmd.MarkFlagRequired("dir") // Mark dir as required for search command
	searchCmd.Flags().StringP("topic", "t", "", "Suffix of the Kafka topic to filter log files (required)")
	_ = searchCmd.MarkFlagRequired("topic") // Mark topic as required for search command

	searchCmd.Flags().Bool("filterId", false, "will only filter ids")
	searchCmd.Flags().Bool("idMatch", false, "will match complete id, to be used along with filterId")

	// Add a flag to the parse command
	parseCmd.Flags().StringP("file", "f", "", "Path to the Kafka log file (required)")
	_ = parseCmd.MarkFlagRequired("file") // Mark file as required for parse command

	// Add subcommands to the root command
	rootCmd.AddCommand(searchCmd, parseCmd)
}
