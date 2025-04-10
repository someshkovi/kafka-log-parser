package main

import (
	"fmt"
)

func main() {
	rootDir := "C:\\Users\\skovi\\Downloads\\bp2-kafka_messages"
	searchString := "bp.nsi.v3.changes.fre"
	logFileExtension := ".log"

	kafka_id_prefix := "::FRE_IP_fd500"

	logFiles, err := searchFiles(rootDir, searchString, logFileExtension)
	if err != nil {
		fmt.Println("Error searching files:", err)
		return
	}

	if len(logFiles) == 0 {
		fmt.Println("No log files found containing the search string.")
	} else {
		fmt.Println("Log files containing the search string:")
		for _, file := range logFiles {
			kafka_events := execute(file, kafka_id_prefix)
			if len(kafka_events) > 0 {
				fmt.Println(file)
				for _, data := range kafka_events {
					fmt.Printf("%+v\n\n", data)
				}
			}
		}
	}
}
