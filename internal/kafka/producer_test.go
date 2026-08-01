package kafka

import (
	"encoding/json"
	"testing"

	"github.com/Logiqode/ThemeParkNFT/internal/models"
)

func TestBuildMessagesWireFormat(t *testing.T) {
	events := []*models.ScanEvent{
		{UserID: "user-0001@bench.local", RideID: "ride-001", Timestamp: 1710000000000, TraceID: "trace-0001"},
		{UserID: "user-0002@bench.local", RideID: "ride-002", Timestamp: 1710000000001, TraceID: "trace-0002"},
	}

	msgs, err := buildMessages(events)
	if err != nil {
		t.Fatalf("buildMessages() error = %v", err)
	}
	if len(msgs) != len(events) {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), len(events))
	}

	for i, msg := range msgs {
		// Message key must be the trace_id so the broker partitions
		// deterministically and the consumer can dedup per-partition.
		if string(msg.Key) != events[i].TraceID {
			t.Errorf("msg[%d].Key = %q, want %q", i, msg.Key, events[i].TraceID)
		}

		// traceparent header must carry the same identity.
		if len(msg.Headers) != 1 || msg.Headers[0].Key != "traceparent" || string(msg.Headers[0].Value) != events[i].TraceID {
			t.Errorf("msg[%d] headers = %+v, want single traceparent=%q", i, msg.Headers, events[i].TraceID)
		}

		// Value must round-trip through the ScanEvent JSON contract unchanged.
		var got models.ScanEvent
		if err := json.Unmarshal(msg.Value, &got); err != nil {
			t.Fatalf("unmarshal msg[%d]: %v", i, err)
		}
		if got != *events[i] {
			t.Errorf("msg[%d] payload = %+v, want %+v", i, got, *events[i])
		}
	}
}

func TestBuildMessagesLargeBatch(t *testing.T) {
	events := make([]*models.ScanEvent, 250)
	for i := range events {
		events[i] = &models.ScanEvent{
			UserID:    "user-" + string(rune('0'+i/100%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10)) + "@bench.local",
			RideID:    "ride-" + string(rune('0'+i%10)),
			Timestamp: int64(1710000000000 + i),
			TraceID:   "trace-batch-000" + string(rune('0'+i/100)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10)),
		}
	}
	msgs, err := buildMessages(events)
	if err != nil {
		t.Fatalf("buildMessages(250) error = %v", err)
	}
	if len(msgs) != 250 {
		t.Fatalf("len(msgs) = %d, want 250", len(msgs))
	}
}