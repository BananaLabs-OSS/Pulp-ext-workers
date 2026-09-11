package workersext

import (
	"context"
	"fmt"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// StatusSignalCapability grants one guest operation: publish a typed status
// signal. It does not grant generic workers, HTTP, endpoint, or credential
// access.
const StatusSignalCapability = "effect.status.signal"

const statusSignalPublishExport = "status_signal_publish"

const (
	statusSignalCodeOK          = 0
	statusSignalCodeInvalid     = 4
	statusSignalCodeExecution   = 5
	statusSignalCodeResponse    = 6
	statusSignalCodeUnavailable = 10
)

func init() {
	ext.Register(newStatusSignalCapability(hostStatusSignalExecutors))
}

func newStatusSignalCapability(factory *ScopedStatusSignalEffectExecutorFactory) ext.Capability {
	return ext.Capability{
		Name:     StatusSignalCapability,
		Provider: "github.com/BananaLabs-OSS/Pulp-ext-workers",
		Setup:    statusSignalSetup,
		Register: func(builder wazero.HostModuleBuilder, cell ext.Cell) error {
			scope, err := ext.ValidatedScopeOf(cell)
			if err != nil {
				return fmt.Errorf("workers status signals: bind scope: %w", err)
			}
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return statusSignalPublish(ctx, module, factory, scope, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(statusSignalPublishExport)
			return nil
		},
		Stub: func(builder wazero.HostModuleBuilder, _ ext.Cell) error {
			builder.NewFunctionBuilder().WithFunc(statusSignalPublishStub).Export(statusSignalPublishExport)
			return nil
		},
		TeardownScope: func(_ context.Context, scope ext.Scope) error {
			if factory != nil {
				if err := factory.TeardownScope(scope); err != nil {
					return err
				}
			}
			return statusSignalTeardown(scope)
		},
	}
}

// status_signal_publish request/response are canonical MessagePack
// effect.Intent/effect.Receipt. The captured scope is host-derived; guest
// bytes cannot select another application, endpoint, or configuration.
func statusSignalPublish(ctx context.Context, module api.Module, factory *ScopedStatusSignalEffectExecutorFactory, scope ext.Scope, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	if requestLen == 0 {
		return codeEmptyReq
	}
	if module == nil || module.Memory() == nil {
		return codeMemRead
	}
	request, ok := module.Memory().Read(requestPtr, requestLen)
	if !ok {
		return codeMemRead
	}
	response, code := executeStatusSignalWire(ctx, factory, scope, request)
	if code != statusSignalCodeOK {
		return code
	}
	if !writeStatusSignalResponse(ctx, module, response, responsePtrPtr, responseLenPtr) {
		return statusSignalCodeResponse
	}
	return statusSignalCodeOK
}

func statusSignalPublishStub(_ context.Context, _ api.Module, _, _, _, _ uint32) uint32 {
	return codeCapAbsent
}

func executeStatusSignalWire(ctx context.Context, factory *ScopedStatusSignalEffectExecutorFactory, scope ext.Scope, request []byte) ([]byte, uint32) {
	if factory == nil {
		return nil, statusSignalCodeUnavailable
	}
	intent, err := effect.UnmarshalIntent(request)
	if err != nil {
		return nil, codeDecode
	}
	if intent.Kind != effect.KindStatusSignalPublish {
		return nil, statusSignalCodeInvalid
	}
	executor, err := factory.ForScope(scope)
	if err != nil {
		return nil, statusSignalCodeUnavailable
	}
	receipt, err := executor.Submit(ctx, scope, intent)
	if err != nil {
		return nil, statusSignalCodeExecution
	}
	wire, err := effect.MarshalReceipt(receipt.Receipt)
	if err != nil {
		return nil, statusSignalCodeResponse
	}
	return wire, statusSignalCodeOK
}

func writeStatusSignalResponse(ctx context.Context, module api.Module, response []byte, responsePtrPtr, responseLenPtr uint32) bool {
	if len(response) == 0 {
		return module.Memory().WriteUint32Le(responsePtrPtr, 0) && module.Memory().WriteUint32Le(responseLenPtr, 0)
	}
	alloc := module.ExportedFunction("pulp_alloc")
	if alloc == nil {
		return false
	}
	result, err := alloc.Call(ctx, uint64(len(response)))
	if err != nil || len(result) == 0 || uint64(len(response)) > uint64(^uint32(0)) {
		return false
	}
	ptr, ok := wasmUint32(result[0])
	if !ok || ptr == 0 {
		return false
	}
	return module.Memory().Write(ptr, response) && module.Memory().WriteUint32Le(responsePtrPtr, ptr) && module.Memory().WriteUint32Le(responseLenPtr, uint32(len(response))) // #nosec G115 -- bounded above.
}
