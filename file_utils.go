package main

import (
	"os"
	"path/filepath"
	"strings"
)

func searchFiles(rootDir string, searchString string, logFileExtension string) ([]string, error) {
	var logFiles []string

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && strings.HasPrefix(info.Name(), searchString) {
			// Walk only this directory
			return filepath.Walk(path, func(subPath string, subInfo os.FileInfo, subErr error) error {
				if subErr != nil {
					return subErr
				}

				// Append the file path to the slice if it's a .log file
				if !subInfo.IsDir() && strings.HasSuffix(subInfo.Name(), logFileExtension) {
					logFiles = append(logFiles, subPath)
				}

				return nil
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return logFiles, nil
}
