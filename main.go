package main

import (
	"fmt"
	"os"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Println("Usage: kafkaDumpExtractorGo <log-file-folder> <kafka-topic-suffix> <fre-id>")
		fmt.Println("sample: kafkaDumpExtractorGo C:skovi\\kafka_9.12.0-k3.6.12_0\\topics bp.ra.v2_0.sync ::FRE_VRF_HubSp-vrf27")
		os.Exit(1)
	}

	rootDir := os.Args[1]
	searchString := os.Args[2]
	kafka_id_prefix := os.Args[3]

	fmt.Println("kafka search string: ", kafka_id_prefix)

	// rootDir := "C:\\Users\\skovi\\Downloads\\bp2-kafka_messages"

	// searchString := "bp.nsi.v3.changes.fre"
	logFileExtension := ".log"

	// kafka_id_prefix := "::FRE_IP_fd500"

	logFiles, err := searchFiles(rootDir, searchString, logFileExtension)
	if err != nil {
		fmt.Println("Error searching files:", err)
		return
	}

	if len(logFiles) == 0 {
		fmt.Println("No log files found with topic:", searchString)
	} else {
		fmt.Println("Log files with topic ", searchString, len(logFiles))
		for _, file := range logFiles {
			kafka_events := execute(file, kafka_id_prefix)
			if len(kafka_events) > 0 {
				fmt.Println(file, len(kafka_events))
				for _, data := range kafka_events {
					fmt.Printf("%+v\n\n", data)
				}
			}
		}
	}
}
