package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// KafkaLogParser parses Kafka binary log files
type KafkaLogParser struct {
	filePath string
}

// RecordBatch represents a Kafka record batch
type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                byte
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsCount         int32
	Records              []Record
}

// Record represents a single Kafka record
type Record struct {
	Attributes     byte
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

// RecordHeader represents a header in a Kafka record
type RecordHeader struct {
	Key   []byte
	Value []byte
}

// NewKafkaLogParser creates a new parser instance
func NewKafkaLogParser(filePath string) *KafkaLogParser {
	return &KafkaLogParser{
		filePath: filePath,
	}
}

// Parse reads and parses the Kafka log file
func (p *KafkaLogParser) Parse() ([]RecordBatch, error) {
	file, err := os.Open(p.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	batches := []RecordBatch{}

	for {
		batch, err := p.parseRecordBatch(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error parsing batch: %v, trying to realign...\n", err)
			p.tryRealign(reader)
			continue
		}

		if batch != nil {
			batches = append(batches, *batch)
		}
	}

	return batches, nil
}

// parseRecordBatch parses a single record batch
func (p *KafkaLogParser) parseRecordBatch(reader *bufio.Reader) (*RecordBatch, error) {
	// Read batch header
	baseOffsetBytes := make([]byte, 8)
	_, err := io.ReadFull(reader, baseOffsetBytes)
	if err != nil {
		return nil, err
	}
	baseOffset := int64(binary.BigEndian.Uint64(baseOffsetBytes))

	batchLengthBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, batchLengthBytes)
	if err != nil {
		return nil, err
	}
	batchLength := int32(binary.BigEndian.Uint32(batchLengthBytes))

	// Check if batch length is reasonable
	if batchLength < 0 || batchLength > 100*1024*1024 { // 100MB max as a sanity check
		return nil, fmt.Errorf("unreasonable batch length: %d", batchLength)
	}

	partitionLeaderEpochBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, partitionLeaderEpochBytes)
	if err != nil {
		return nil, err
	}
	partitionLeaderEpoch := int32(binary.BigEndian.Uint32(partitionLeaderEpochBytes))

	magicByte := make([]byte, 1)
	_, err = io.ReadFull(reader, magicByte)
	if err != nil {
		return nil, err
	}
	magic := magicByte[0]

	// Only continue parsing for magic byte 2 (Kafka 0.11+)
	if magic != 2 {
		fmt.Printf("Encountered older Kafka log format: magic byte %d\n", magic)
		// Skip the rest of this batch
		_, err = io.CopyN(io.Discard, reader, int64(batchLength-9)) // -9 for fields already read
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Parse record batch for magic byte 2
	crcBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, crcBytes)
	if err != nil {
		return nil, err
	}
	crc := binary.BigEndian.Uint32(crcBytes)

	attributesBytes := make([]byte, 2)
	_, err = io.ReadFull(reader, attributesBytes)
	if err != nil {
		return nil, err
	}
	attributes := int16(binary.BigEndian.Uint16(attributesBytes))

	lastOffsetDeltaBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, lastOffsetDeltaBytes)
	if err != nil {
		return nil, err
	}
	lastOffsetDelta := int32(binary.BigEndian.Uint32(lastOffsetDeltaBytes))

	firstTimestampBytes := make([]byte, 8)
	_, err = io.ReadFull(reader, firstTimestampBytes)
	if err != nil {
		return nil, err
	}
	firstTimestamp := int64(binary.BigEndian.Uint64(firstTimestampBytes))

	maxTimestampBytes := make([]byte, 8)
	_, err = io.ReadFull(reader, maxTimestampBytes)
	if err != nil {
		return nil, err
	}
	maxTimestamp := int64(binary.BigEndian.Uint64(maxTimestampBytes))

	producerIDBytes := make([]byte, 8)
	_, err = io.ReadFull(reader, producerIDBytes)
	if err != nil {
		return nil, err
	}
	producerID := int64(binary.BigEndian.Uint64(producerIDBytes))

	producerEpochBytes := make([]byte, 2)
	_, err = io.ReadFull(reader, producerEpochBytes)
	if err != nil {
		return nil, err
	}
	producerEpoch := int16(binary.BigEndian.Uint16(producerEpochBytes))

	baseSequenceBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, baseSequenceBytes)
	if err != nil {
		return nil, err
	}
	baseSequence := int32(binary.BigEndian.Uint32(baseSequenceBytes))

	recordsCountBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, recordsCountBytes)
	if err != nil {
		return nil, err
	}
	recordsCount := int32(binary.BigEndian.Uint32(recordsCountBytes))

	// Create record batch
	batch := &RecordBatch{
		BaseOffset:           baseOffset,
		BatchLength:          batchLength,
		PartitionLeaderEpoch: partitionLeaderEpoch,
		Magic:                magic,
		CRC:                  crc,
		Attributes:           attributes,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       firstTimestamp,
		MaxTimestamp:         maxTimestamp,
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		RecordsCount:         recordsCount,
		Records:              make([]Record, 0, recordsCount),
	}

	// Parse records
	for i := int32(0); i < recordsCount; i++ {
		record, err := p.parseRecord(reader, magic)
		if err != nil {
			return nil, fmt.Errorf("error parsing record %d: %w", i, err)
		}
		batch.Records = append(batch.Records, *record)
	}

	return batch, nil
}

// parseRecord parses a single record
func (p *KafkaLogParser) parseRecord(reader *bufio.Reader, magic byte) (*Record, error) {
	if magic != 2 {
		return nil, fmt.Errorf("unsupported magic byte: %d", magic)
	}

	// For magic byte 2, read variable-length encoded record
	_, err := p.readVarint(reader)
	if err != nil {
		return nil, err
	}

	attributeByte := make([]byte, 1)
	_, err = io.ReadFull(reader, attributeByte)
	if err != nil {
		return nil, err
	}

	timestampDelta, err := p.readVarint(reader)
	if err != nil {
		return nil, err
	}

	offsetDelta, err := p.readVarint(reader)
	if err != nil {
		return nil, err
	}

	// Read key
	keyLength, err := p.readVarint(reader)
	if err != nil {
		return nil, err
	}

	var key []byte
	if keyLength >= 0 {
		key = make([]byte, keyLength)
		_, err = io.ReadFull(reader, key)
		if err != nil {
			return nil, err
		}
	}

	// Read value
	valueLength, err := p.readVarint(reader)
	if err != nil {
		return nil, err
	}

	var value []byte
	if valueLength >= 0 {
		value = make([]byte, valueLength)
		_, err = io.ReadFull(reader, value)
		if err != nil {
			return nil, err
		}
	}

	// Read headers
	headerCount, err := p.readVarint(reader)
	if err != nil {
		return nil, err
	}

	headers := make([]RecordHeader, 0, headerCount)
	for i := int64(0); i < headerCount; i++ {
		headerKeyLength, err := p.readVarint(reader)
		if err != nil {
			return nil, err
		}

		headerKey := make([]byte, headerKeyLength)
		_, err = io.ReadFull(reader, headerKey)
		if err != nil {
			return nil, err
		}

		headerValueLength, err := p.readVarint(reader)
		if err != nil {
			return nil, err
		}

		var headerValue []byte
		if headerValueLength >= 0 {
			headerValue = make([]byte, headerValueLength)
			_, err = io.ReadFull(reader, headerValue)
			if err != nil {
				return nil, err
			}
		}

		headers = append(headers, RecordHeader{
			Key:   headerKey,
			Value: headerValue,
		})
	}

	return &Record{
		Attributes:     attributeByte[0],
		TimestampDelta: timestampDelta,
		OffsetDelta:    offsetDelta,
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil
}

// readVarint reads a variable-length integer
func (p *KafkaLogParser) readVarint(reader *bufio.Reader) (int64, error) {
	var value int64
	var shift uint

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		value |= int64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}

	// Handle zigzag encoding (used for signed varints in Kafka)
	return (value >> 1) ^ (-(value & 1)), nil
}

// tryRealign attempts to find the next valid record batch
func (p *KafkaLogParser) tryRealign(reader *bufio.Reader) error {
	// Skip one byte at a time until we might find a valid record
	_, err := reader.ReadByte()
	return err
}

// PrintBatchSummary prints a summary of the parsed batches
func PrintBatchSummary(batches []RecordBatch) {
	fmt.Printf("Found %d record batches\n", len(batches))
	for i, batch := range batches {
		fmt.Printf("Batch %d: BaseOffset=%d, Records=%d\n",
			i, batch.BaseOffset, batch.RecordsCount)

		// Print a sample of records
		maxRecords := 5
		if int(batch.RecordsCount) < maxRecords {
			maxRecords = int(batch.RecordsCount)
		}

		for j := 0; j < maxRecords; j++ {
			record := batch.Records[j]
			// fmt.Printf("  Record %d: Offset=%d, Key=%v\n",
			// 	j, record.OffsetDelta, record.Key)
			fmt.Printf("  Record %d: Offset=%d, Key=%s\n",
				j, record.OffsetDelta, string(record.Key))

			// Try to print value as string if possible
			if record.Value != nil {
				if len(record.Value) > 50 {
					fmt.Printf("    Value: %s... (truncated, %d bytes total)\n",
						string(record.Value[:50]), len(record.Value))
				} else {
					fmt.Printf("    Value: %s\n", string(record.Value))
				}
			} else {
				fmt.Printf("    Value: nil\n")
			}
		}

		if int(batch.RecordsCount) > maxRecords {
			fmt.Printf("  ... %d more records\n", batch.RecordsCount-int32(maxRecords))
		}
		fmt.Println()
	}
}

type JSONData struct {
	Header struct {
		Timestamp time.Time `json:"timestamp"`
	} `json:"header"`
	Event struct {
		Type        string                 `json:"_type"`
		Op          string                 `json:"op"`
		ObjectID    string                 `json:"object_id"`
		ObjectType  string                 `json:"object_type"`
		ObjectData  map[string]interface{} `json:"object_data"`
		ObjectPatch map[string]interface{} `json:"object_patch"`
	} `json:"event"`
}

func getBatchSummary(batches []RecordBatch, filterString string) {
	var jsonDataList []JSONData
	for _, batch := range batches {

		for _, record := range batch.Records {
			if record.Value != nil {
				// fmt.Printf("%s \n%s \n", string(record.Key), string(record.Value))
				var jsonData JSONData
				err := json.Unmarshal([]byte(record.Value), &jsonData)
				if err != nil {
					fmt.Println("Error unmarshaling JSON:", err)
					return
				}
				// Filter the data based on ObjectID ending with a particular string
				// could also be done based on record.key
				if strings.HasSuffix(jsonData.Event.ObjectID, filterString) {
					jsonDataList = append(jsonDataList, jsonData)
				}

			}

		}
	}
	// Sort the data based on Timestamp
	sort.Slice(jsonDataList, func(i, j int) bool {
		return jsonDataList[i].Header.Timestamp.Before(jsonDataList[j].Header.Timestamp)
	})

	// Print the filtered and sorted data
	for _, data := range jsonDataList {
		fmt.Printf("%+v\n\n", data)
	}
}

func getBatchSummarySimplified(batches []RecordBatch, filterString string) []string {
	var jsonDataList []string
	for _, batch := range batches {

		for _, record := range batch.Records {
			if record.Value != nil {
				// Filter the data based on ObjectID ending with a particular string
				if strings.HasSuffix(string(record.Key), filterString) {
					jsonDataList = append(jsonDataList, string(record.Value))
				}
			}
		}
	}
	// Print the filtered and sorted data
	// for _, data := range jsonDataList {
	// 	fmt.Printf("%+v\n\n", data)
	// }
	return jsonDataList
}

func execute(logFilePath string, id_prefix string) []string {
	// if len(os.Args) < 2 {
	// 	fmt.Println("Usage: kafka-log-parser <log-file-path>")
	// 	os.Exit(1)
	// }

	// logFilePath := os.Args[1]
	// logFilePath := "tests\\00000000000000000000.log"
	parser := NewKafkaLogParser(logFilePath)

	batches, err := parser.Parse()
	if err != nil {
		fmt.Printf("Error parsing log file: %v\n", err)
		os.Exit(1)
	}

	return getBatchSummarySimplified(batches, id_prefix)
}
