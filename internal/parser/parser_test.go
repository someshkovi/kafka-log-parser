package parser

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogParserExecute(t *testing.T) {

	logFilePath := filepath.Join("tests", "bp.nsi.v3.changes.fre", "00000000000000000000.log")
	id_prefix := "11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500"

	inputParams := DefaultInputParams()

	got := Execute(logFilePath, id_prefix, inputParams)
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

func TestEventMapping(t *testing.T) {
	event := `{"version":1,"header":{"envelopeId":"10aa0ada-75d7-413d-9968-852372f5d960","timestamp":"2025-03-27T03:59:49.031Z","traceId":"bbbf4408-f1d3-495a-b75a-885d91d69d71","upstreamId":"3deac7fc-f262-44f8-94c1-746114b793bb"},"event":{"_type":"bp.v1.ObjectChanged","op":"updated","object_id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500","object_type":"com.ciena.bp.nsi.api.v2_0.fre.FreRO","object_data":{"data":{"id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500","type":"fres","attributes":{"operationState":"fully operating","deploymentState":"discovered","displayData":{"operationState":"Up","displayDeploymentState":"Discovered"},"resourceState":"discovered","serviceClass":"IP","lastUpdatedAdminStateTimeStamp":"2025-03-27T03:32:40.714Z","userLabel":"fd500","mgmtName":"fd500","nativeName":"fd500","layerRate":"ETHERNET","multiHighestStackLayerRate":"ETHERNET","networkRole":"IFRE","directionality":"bidirectional","topologySources":["discovered"],"signalContentType":"IP","active":true,"additionalAttributes":{"isActual":"true"},"resilienceLevel":"unprotected"},"relationships":{"freDiscovered":{"data":{"type":"freDiscovered","id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500"}},"endPoints":{"data":[{"type":"endPoints","id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500::EP0"},{"type":"endPoints","id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500::EP1"}]},"networkConstruct":{"data":{"type":"networkConstructs","id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1"}}}},"included":[{"id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500::EP0","type":"endPoints","attributes":{"role":"symmetric","directionality":"bidirectional"},"relationships":{"tpes":{"data":[{"type":"tpes","id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::TPE_FTP_IPDATA_intf11_1"}]}}},{"id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::FRE_IP_fd500::EP1","type":"endPoints","attributes":{"role":"symmetric","directionality":"bidirectional"},"relationships":{"tpes":{"data":[{"type":"tpes","id":"11648c51-49de-3a40-bcdd-d1cd1764dcc1::TPE_11_CTPServerToClient_SUBPORT_fp500"}]}}}]},"object_patch":{}}}`
	var eventStruct JSONData
	err := json.Unmarshal([]byte(event), &eventStruct)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	jsonData, err := json.Marshal(eventStruct)
	if err != nil {
		fmt.Printf("Error marshaling JSON: %s", err)
	}

	fmt.Println(string(jsonData))
}
