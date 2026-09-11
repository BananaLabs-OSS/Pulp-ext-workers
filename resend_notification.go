package workersext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/abi"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	defaultResendEndpoint = "https://api.resend.com/emails"
	defaultResendFrom     = "Sessions <noreply@sessions.gg>"
)

// ResendNotificationEmailConfig is supplied explicitly by deployment wiring.
// The extension never reads environment variables for provider credentials.
// APIKey is intentionally never logged or included in an effect receipt.
type ResendNotificationEmailConfig struct {
	APIKey   string
	Endpoint string
	From     string
	Timeout  time.Duration
}

// ResendNotificationEmailConfigSource supplies explicit provider configuration
// for one application/cell placement. It is intentionally a deployment-owned
// function rather than an environment lookup inside this extension.
type ResendNotificationEmailConfigSource func(ext.Scope) (ResendNotificationEmailConfig, error)

// NotificationEmailPayload is the strict kind-owned MessagePack payload for
// pulp.effect.notification.email.send.v1. It contains rendered delivery
// content; templates and state stay in the cell that emitted the intent.
type NotificationEmailPayload struct {
	To      string `msgpack:"to"`
	Subject string `msgpack:"subject"`
	HTML    string `msgpack:"html,omitempty"`
	Text    string `msgpack:"text,omitempty"`
	ReplyTo string `msgpack:"reply_to,omitempty"`
}

// NotificationEmailResult is the canonical MessagePack result returned on a
// successful provider acknowledgement. It deliberately excludes message body,
// authorization, and untrusted provider diagnostics.
type NotificationEmailResult struct {
	Provider  string `msgpack:"provider"`
	MessageID string `msgpack:"message_id,omitempty"`
}

// ResendNotificationEmailDelivery is a real host-side notification adapter.
// It is invoked by EffectExecutor from the already-acquired scoped worker
// queue; DeliverNotificationEmail does not create its own goroutine.
type ResendNotificationEmailDelivery struct {
	config ResendNotificationEmailConfig
	http   resendHTTPWorker
}

type resendHTTPWorker interface {
	Fetch(context.Context, taskRequest) ([]byte, error)
}

type pooledResendHTTPWorker struct{ pool *workerPool }

func (w pooledResendHTTPWorker) Fetch(ctx context.Context, request taskRequest) ([]byte, error) {
	if w.pool == nil {
		return nil, errors.New("workers resend: worker pool is unavailable")
	}
	return w.pool.doHTTPFetch(ctx, request)
}

// NewResendNotificationEmailDelivery constructs an explicit, scope-validated
// provider adapter. The shared Workers runtime must already exist; durable
// receipt storage is supplied separately by the scoped executor factory, so
// deployments with an explicit storage resolver do not need to register a
// second ambient storage root for every owner cell.
func NewResendNotificationEmailDelivery(scope ext.Scope, config ResendNotificationEmailConfig) (*ResendNotificationEmailDelivery, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("workers resend: scope: %w", err)
	}
	p := sharedWorkerPool()
	if p == nil {
		return nil, errors.New("workers resend: worker pool is unavailable")
	}
	return newResendNotificationEmailDelivery(config, pooledResendHTTPWorker{pool: p})
}

// NewResendScopedNotificationEffectExecutorFactory is the production wiring
// helper for a deployment that uses Resend. It retains all fail-closed and
// durable-store guarantees of ScopedNotificationEffectExecutorFactory while
// keeping provider configuration outside the extension.
func NewResendScopedNotificationEffectExecutorFactory(configSource ResendNotificationEmailConfigSource) (*ScopedNotificationEffectExecutorFactory, error) {
	return NewResendScopedNotificationEffectExecutorFactoryWithStorage(configSource, func(scope ext.Scope) (string, error) {
		root, ok := workersStorageRoot(scope)
		if !ok {
			return "", errors.New("workers notification effects: application storage root is unavailable")
		}
		return root, nil
	})
}

func NewResendScopedNotificationEffectExecutorFactoryWithStorage(configSource ResendNotificationEmailConfigSource, storageRoot NotificationEffectStorageRoot) (*ScopedNotificationEffectExecutorFactory, error) {
	if configSource == nil {
		return nil, errors.New("workers resend: config source is required")
	}
	return NewScopedNotificationEffectExecutorFactoryWithStorage(func(scope ext.Scope) (NotificationEmailDelivery, error) {
		config, err := configSource(scope)
		if err != nil {
			return nil, fmt.Errorf("workers resend: provider config: %w", err)
		}
		p := sharedWorkerPool()
		if p == nil {
			return nil, errors.New("workers resend: worker pool is unavailable")
		}
		return newResendNotificationEmailDelivery(config, pooledResendHTTPWorker{pool: p})
	}, storageRoot)
}

func newResendNotificationEmailDelivery(config ResendNotificationEmailConfig, worker resendHTTPWorker) (*ResendNotificationEmailDelivery, error) {
	if worker == nil {
		return nil, errors.New("workers resend: HTTP worker is required")
	}
	config = normalizeResendConfig(config)
	if err := validateResendConfig(config); err != nil {
		return nil, err
	}
	return &ResendNotificationEmailDelivery{config: config, http: worker}, nil
}

func normalizeResendConfig(config ResendNotificationEmailConfig) ResendNotificationEmailConfig {
	if config.Endpoint == "" {
		config.Endpoint = defaultResendEndpoint
	}
	if config.From == "" {
		config.From = defaultResendFrom
	}
	if config.Timeout <= 0 {
		config.Timeout = defaultFetchTimeout
	}
	return config
}

func validateResendConfig(config ResendNotificationEmailConfig) error {
	if strings.TrimSpace(config.APIKey) == "" {
		return errors.New("workers resend: API key is required")
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil {
		return errors.New("workers resend: endpoint must be an absolute https URL")
	}
	if _, err := mail.ParseAddress(config.From); err != nil {
		return errors.New("workers resend: from address is invalid")
	}
	if config.Timeout > maxFetchTimeout {
		return fmt.Errorf("workers resend: timeout exceeds %s", maxFetchTimeout)
	}
	return nil
}

func (d *ResendNotificationEmailDelivery) DeliverNotificationEmail(ctx context.Context, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
	if d == nil || d.http == nil {
		return nil, genericFailure("host_unavailable", "notification delivery is unavailable"), nil
	}
	if err := intent.Validate(); err != nil || intent.Kind != effect.KindNotificationEmailSend {
		return nil, genericFailure("invalid_notification_intent", "notification intent is invalid"), nil
	}
	payload, failure := decodeNotificationEmailPayload(intent.Payload)
	if failure != nil {
		return nil, failure, nil
	}
	body, err := json.Marshal(resendRequest{
		From: d.config.From, To: []string{payload.To}, Subject: payload.Subject,
		HTML: payload.HTML, Text: payload.Text, ReplyTo: payload.ReplyTo,
	})
	if err != nil {
		return nil, genericFailure("notification_encoding_failed", "notification could not be encoded"), nil
	}
	timeoutMillis := d.config.Timeout.Milliseconds()
	if timeoutMillis < 0 || uint64(timeoutMillis) > uint64(^uint32(0)) {
		return nil, genericFailure("invalid_notification_timeout", "notification timeout is outside the supported range"), nil
	}
	timeoutMS := uint32(timeoutMillis) // #nosec G115 -- bounded above.
	data, err := d.http.Fetch(ctx, taskRequest{
		Type: "http.fetch", Method: "POST", URL: d.config.Endpoint, Body: body, TimeoutMs: timeoutMS,
		Headers: map[string]string{
			"Authorization":   "Bearer " + d.config.APIKey,
			"Content-Type":    "application/json",
			"Idempotency-Key": intent.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, genericFailure("notification_provider_unavailable", "notification provider is unavailable"), nil
	}
	response, err := abi.DecodeHTTPResponse(data)
	if err != nil {
		return nil, genericFailure("notification_provider_invalid_response", "notification provider returned an invalid response"), nil
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, genericFailure("notification_provider_rejected", "notification provider rejected delivery"), nil
	}
	var providerResponse struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(response.Body, &providerResponse)
	result, err := msgpack.Marshal(NotificationEmailResult{Provider: "resend", MessageID: providerResponse.ID})
	if err != nil {
		return nil, genericFailure("notification_result_encoding_failed", "notification result could not be encoded"), nil
	}
	return result, nil, nil
}

type resendRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	HTML    string   `json:"html,omitempty"`
	Text    string   `json:"text,omitempty"`
	ReplyTo string   `json:"reply_to,omitempty"`
}

func decodeNotificationEmailPayload(raw msgpack.RawMessage) (NotificationEmailPayload, *effect.Failure) {
	var payload NotificationEmailPayload
	if err := msgpack.Unmarshal(raw, &payload); err != nil {
		return NotificationEmailPayload{}, genericFailure("invalid_notification_payload", "notification payload is invalid")
	}
	if strings.TrimSpace(payload.To) != payload.To || strings.TrimSpace(payload.Subject) != payload.Subject || payload.To == "" || payload.Subject == "" {
		return NotificationEmailPayload{}, genericFailure("invalid_notification_payload", "notification payload is invalid")
	}
	if strings.ContainsAny(payload.Subject, "\r\n") || (payload.HTML == "" && payload.Text == "") {
		return NotificationEmailPayload{}, genericFailure("invalid_notification_payload", "notification payload is invalid")
	}
	if _, err := mail.ParseAddress(payload.To); err != nil {
		return NotificationEmailPayload{}, genericFailure("invalid_notification_payload", "notification payload is invalid")
	}
	if payload.ReplyTo != "" {
		if strings.TrimSpace(payload.ReplyTo) != payload.ReplyTo {
			return NotificationEmailPayload{}, genericFailure("invalid_notification_payload", "notification payload is invalid")
		}
		if _, err := mail.ParseAddress(payload.ReplyTo); err != nil {
			return NotificationEmailPayload{}, genericFailure("invalid_notification_payload", "notification payload is invalid")
		}
	}
	return payload, nil
}
