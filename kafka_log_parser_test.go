package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLogParserExecute(t *testing.T) {

	logFilePath := filepath.Join("tests", "bp.nsi.v3.changes.fre", "00000000000000000000.log")
	id_prefix := "11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500"

	got := execute(logFilePath, id_prefix)
	// want := []string{"rest"}
	length := 4

	if len(got) != length {
		t.Errorf("got %q, wanted %q", got, length)
	}

	event_id_string := "\"object_id\":\"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500\""
	if !strings.Contains(got[0], event_id_string) {
		t.Errorf("got %q, wanted %q", got, length)
	}
}
