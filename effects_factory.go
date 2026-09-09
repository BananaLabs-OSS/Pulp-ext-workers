package workersext

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/vmihailenco/msgpack/v5"
)

// NotificationEmailDelivery is the explicit privileged adapter for canonical
// notification-email intents. A deployment provides it; this extension never
// loads provider credentials, reads dotenv files, or invents a network client.
// Implementations must use intent.IdempotencyKey at their provider boundary
// whenever the provider supports stable idempotency.
type NotificationEmailDelivery interface {
	DeliverNotificationEmail(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error)
}

// NotificationEmailDeliveryFunc adapts a function into a host-owned delivery
// implementation, which keeps deployment wiring concise without a default
// no-op sender.
type NotificationEmailDeliveryFunc func(context.Context, effect.Intent) (msgpack.RawMessage, *effect.Failure, error)

func (f NotificationEmailDeliveryFunc) DeliverNotificationEmail(ctx context.Context, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
	return f(ctx, intent)
}

// NotificationEmailHandlerFactory creates one real privileged adapter per
// Pulp placement. It must return an error when that placement lacks delivery
// configuration; the effect dispatcher will then retain and retry the outbox
// record rather than silently acknowledging an undelivered email.
type NotificationEmailHandlerFactory func(ext.Scope) (NotificationEmailDelivery, error)
type NotificationEffectStorageRoot func(ext.Scope) (string, error)

// ScopedNotificationEffectExecutorFactory constructs one durable executor per
// application/cell instance. Its store root comes from an explicit host-owned
// resolver (SetupEnv by default), and its delivery adapter is host-owned. There is no
// MemoryEffectStore or default sender on this production path.
type ScopedNotificationEffectExecutorFactory struct {
	mu             sync.Mutex
	handlerFactory NotificationEmailHandlerFactory
	storageRoot    NotificationEffectStorageRoot
	storeFactory   EffectStoreFactory
	executors      map[ext.ResourceKey]*EffectExecutor
}

func NewScopedNotificationEffectExecutorFactory(handlerFactory NotificationEmailHandlerFactory) (*ScopedNotificationEffectExecutorFactory, error) {
	return NewScopedNotificationEffectExecutorFactoryWithStorage(handlerFactory, func(scope ext.Scope) (string, error) {
		root, ok := workersStorageRoot(scope)
		if !ok {
			return "", errors.New("workers notification effects: application storage root is unavailable")
		}
		return root, nil
	})
}

func NewScopedNotificationEffectExecutorFactoryWithStorage(handlerFactory NotificationEmailHandlerFactory, storageRoot NotificationEffectStorageRoot) (*ScopedNotificationEffectExecutorFactory, error) {
	if handlerFactory == nil {
		return nil, errors.New("workers notification effects: handler factory is required")
	}
	if storageRoot == nil {
		return nil, errors.New("workers notification effects: storage root source is required")
	}
	return &ScopedNotificationEffectExecutorFactory{
		handlerFactory: handlerFactory,
		storageRoot:    storageRoot,
		executors:      make(map[ext.ResourceKey]*EffectExecutor),
	}, nil
}

// NewScopedNotificationEffectExecutorFactoryWithStore binds production to a
// host-owned durable store without exposing database configuration to cells.
func NewScopedNotificationEffectExecutorFactoryWithStore(handlerFactory NotificationEmailHandlerFactory, storeFactory EffectStoreFactory) (*ScopedNotificationEffectExecutorFactory, error) {
	if handlerFactory == nil {
		return nil, errors.New("workers notification effects: handler factory is required")
	}
	if storeFactory == nil {
		return nil, errors.New("workers notification effects: store factory is required")
	}
	return &ScopedNotificationEffectExecutorFactory{handlerFactory: handlerFactory, storeFactory: storeFactory, executors: make(map[ext.ResourceKey]*EffectExecutor)}, nil
}

func (f *ScopedNotificationEffectExecutorFactory) ForScope(scope ext.Scope) (*EffectExecutor, error) {
	if f == nil || f.handlerFactory == nil || (f.storageRoot == nil && f.storeFactory == nil) {
		return nil, errors.New("workers notification effects: factory is not configured")
	}
	key, err := scope.ResourceKey("workers-notification-effect", "executor")
	if err != nil {
		return nil, fmt.Errorf("workers notification effects: scope: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if executor := f.executors[key]; executor != nil {
		return executor, nil
	}
	var store EffectStore
	if f.storeFactory != nil {
		store, err = f.storeFactory(scope, "workers-notification-effect")
	} else {
		var storageRoot string
		storageRoot, err = f.storageRoot(scope)
		if err == nil {
			store, err = NewFileEffectStore(storageRoot, scope)
		}
	}
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("workers notification effects: effect store is nil")
	}
	delivery, err := f.handlerFactory(scope)
	if err != nil {
		return nil, fmt.Errorf("workers notification effects: delivery adapter: %w", err)
	}
	if delivery == nil {
		return nil, errors.New("workers notification effects: delivery adapter is nil")
	}
	handler := EffectHandler(func(ctx context.Context, intent effect.Intent) (msgpack.RawMessage, *effect.Failure, error) {
		if intent.Kind != effect.KindNotificationEmailSend {
			return nil, nil, fmt.Errorf("workers notification effects: unsupported kind %q", intent.Kind)
		}
		return delivery.DeliverNotificationEmail(ctx, intent)
	})
	executor, err := NewHostEffectExecutor(store, handler)
	if err != nil {
		return nil, fmt.Errorf("workers notification effects: executor: %w", err)
	}
	f.executors[key] = executor
	return executor, nil
}

// TeardownScope drops only executors belonging to one application instance.
// Their durable receipt files remain so an intentional restart can replay a
// terminal provider receipt instead of issuing another email.
func (f *ScopedNotificationEffectExecutorFactory) TeardownScope(scope ext.Scope) error {
	if f == nil {
		return nil
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.executors {
		owner := key.Scope()
		if sameApplication(owner, scope) {
			delete(f.executors, key)
		}
	}
	return nil
}

func (f *ScopedNotificationEffectExecutorFactory) Count() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.executors)
}
