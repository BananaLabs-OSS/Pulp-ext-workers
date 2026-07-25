package workersext

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/abi"
	"github.com/vmihailenco/msgpack/v5"
)

type fakeResendHTTPWorker struct {
	request taskRequest
	data    []byte
	err     error
}

func (w *fakeResendHTTPWorker) Fetch(_ context.Context, request taskRequest) ([]byte, error) {
	w.request = request
	return w.data, w.err
}

func resendIntent(t *testing.T, key string, payload NotificationEmailPayload) effect.Intent {
	t.Helper()
	wire, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return effect.Intent{
		Version: effect.VersionV1, ID: "notification-1", Kind: effect.KindNotificationEmailSend,
		IdempotencyKey: key, Payload: wire,
	}
}

func fakeResendResponse(t *testing.T, status uint32, body any) []byte {
	t.Helper()
	jsonBody, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := abi.EncodeHTTPResponse(abi.HTTPResponse{Status: status, Body: jsonBody})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestResendNotificationDelivery_UsesScopedWorkerRequestAndStableIdempotency(t *testing.T) {
	worker := &fakeResendHTTPWorker{data: fakeResendResponse(t, 202, map[string]string{"id": "email_123"})}
	delivery, err := newResendNotificationEmailDelivery(ResendNotificationEmailConfig{APIKey: "test-only", Timeout: time.Second}, worker)
	if err != nil {
		t.Fatalf("new delivery: %v", err)
	}
	intent := resendIntent(t, "order-1:ready-email", NotificationEmailPayload{
		To: "player@example.com", Subject: "Your Session is ready", HTML: "<p>Ready</p>",
	})
	result, failure, err := delivery.DeliverNotificationEmail(context.Background(), intent)
	if err != nil || failure != nil {
		t.Fatalf("delivery = (%x, %#v, %v)", result, failure, err)
	}
	if worker.request.Type != "http.fetch" || worker.request.Method != "POST" || worker.request.URL != defaultResendEndpoint {
		t.Fatalf("worker request = %#v", worker.request)
	}
	if worker.request.Headers["Idempotency-Key"] != intent.IdempotencyKey {
		t.Fatalf("idempotency header = %q, want %q", worker.request.Headers["Idempotency-Key"], intent.IdempotencyKey)
	}
	if worker.request.Headers["Authorization"] == "" || worker.request.Headers["Content-Type"] != "application/json" {
		t.Fatal("provider request omitted authorization or content type")
	}
	decoded, err := effect.DecodeResult[NotificationEmailResult](effect.Receipt{
		Version: effect.VersionV1, IntentID: intent.ID, Kind: intent.Kind, IdempotencyKey: intent.IdempotencyKey,
		Status: effect.Completed, Result: result,
	})
	if err != nil || decoded.Provider != "resend" || decoded.MessageID != "email_123" {
		t.Fatalf("delivery result = %#v, %v", decoded, err)
	}
}

func TestResendNotificationDelivery_SanitizesProviderAndPayloadFailures(t *testing.T) {
	intent := resendIntent(t, "order-2:ready-email", NotificationEmailPayload{
		To: "player@example.com", Subject: "Ready", Text: "Ready",
	})
	for name, worker := range map[string]*fakeResendHTTPWorker{
		"rejected":    {data: fakeResendResponse(t, 422, map[string]string{"message": "do not surface this"})},
		"unavailable": {err: errors.New("do not surface this")},
	} {
		t.Run(name, func(t *testing.T) {
			delivery, err := newResendNotificationEmailDelivery(ResendNotificationEmailConfig{APIKey: "test-only"}, worker)
			if err != nil {
				t.Fatal(err)
			}
			_, failure, err := delivery.DeliverNotificationEmail(context.Background(), intent)
			if err != nil || failure == nil || failure.Message == "do not surface this" {
				t.Fatalf("failure = %#v, err=%v", failure, err)
			}
		})
	}

	worker := &fakeResendHTTPWorker{data: fakeResendResponse(t, 202, map[string]string{"id": "ignored"})}
	delivery, err := newResendNotificationEmailDelivery(ResendNotificationEmailConfig{APIKey: "test-only"}, worker)
	if err != nil {
		t.Fatal(err)
	}
	bad := resendIntent(t, "order-3:ready-email", NotificationEmailPayload{To: "bad\naddress", Subject: "Ready", Text: "Ready"})
	if _, failure, err := delivery.DeliverNotificationEmail(context.Background(), bad); err != nil || failure == nil || failure.Code != "invalid_notification_payload" {
		t.Fatalf("payload failure = %#v, err=%v", failure, err)
	}
	if worker.request.URL != "" {
		t.Fatal("invalid payload reached HTTP worker")
	}
}

func TestResendNotificationDelivery_ConfigFailsClosed(t *testing.T) {
	worker := &fakeResendHTTPWorker{}
	for name, config := range map[string]ResendNotificationEmailConfig{
		"missing key":   {Endpoint: defaultResendEndpoint},
		"http endpoint": {APIKey: "test-only", Endpoint: "http://example.test/emails"},
		"invalid from":  {APIKey: "test-only", From: "not an address"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newResendNotificationEmailDelivery(config, worker); err == nil {
				t.Fatal("invalid provider config accepted")
			}
		})
	}
}
