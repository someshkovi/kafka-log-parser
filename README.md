# Extract kafka messages from binary log files.

## Initalize
```sh
go get -u github.com/spf13/cobra
go install
```

## Usage: 
```sh
kafka-log-parser search -d <log_folder_path> -t <topic_or_suffix_of_folders_to_filter> -i <id_to_filter_from_string>
```
