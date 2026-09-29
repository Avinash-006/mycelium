package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	mycelium "github.com/mycelium-labs/mycelium/clients/go"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("missing " + name)
	}
	return value
}

func identity(
	requestID string,
	tenant mycelium.TenantID,
	application mycelium.ApplicationID,
) mycelium.IdentityRequest {
	return mycelium.IdentityRequest{
		ApplicationID:           application,
		BusinessRequestID:       mycelium.BusinessRequestID(requestID),
		CanonicalizationVersion: "jcs-1",
		Destination:             mycelium.Destination{"id": "record-9", "kind": "record"},
		ExecutionScope:          mycelium.ExecutionScope{"entity": "record-9", "tenant": string(tenant)},
		IdentityVersion:         "1",
		Input:                   map[string]any{"operation": "update", "value": "new-value"},
		TenantID:                tenant,
		ToolContractVersion:     "1",
		ToolID:                  "external_operation",
	}
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func requireProtocolError(err error, code string) *mycelium.ProtocolError {
	if err == nil {
		panic("expected protocol error " + code)
	}
	var protocol *mycelium.ProtocolError
	if !errors.As(err, &protocol) {
		panic(fmt.Sprintf("expected ProtocolError, got %T: %v", err, err))
	}
	if protocol.Code != code {
		panic(fmt.Sprintf("expected protocol code %s, got %s", code, protocol.Code))
	}
	return protocol
}

func main() {
	ctx := context.Background()
	baseURL := required("MYCELIUM_CONFORMANCE_URL")
	token := required("MYCELIUM_CONFORMANCE_TOKEN")
	tenant := mycelium.TenantID(required("MYCELIUM_CONFORMANCE_TENANT"))
	application := mycelium.ApplicationID(required("MYCELIUM_CONFORMANCE_APPLICATION"))
	expectedEffectID := mycelium.EffectID(required("MYCELIUM_CONFORMANCE_EFFECT_ID"))
	checks := []string{}

	client := must(mycelium.NewClient(mycelium.ClientOptions{
		BaseURL: baseURL, Token: token, TenantID: tenant, ApplicationID: application,
	}))
	if err := client.AssertCompatible(ctx); err != nil {
		panic(err)
	}
	checks = append(checks, "capabilities")

	derived := must(client.DeriveEffectIdentity(ctx, identity("request-884", tenant, application)))
	if derived.EffectID != expectedEffectID {
		panic(fmt.Sprintf("identity fixture mismatch: %s", derived.EffectID))
	}
	checks = append(checks, "identity-fixture")

	unauthorized := must(mycelium.NewClient(mycelium.ClientOptions{
		BaseURL: baseURL, Token: strings.Repeat("x", 43), TenantID: tenant, ApplicationID: application,
	}))
	_, err := unauthorized.Capabilities(ctx)
	requireProtocolError(err, "AUTHENTICATION_INVALID")
	checks = append(checks, "authentication")

	decision := &mycelium.Decision{
		Allowed: true, Verdicts: []mycelium.Verdict{}, DeniedReasons: []string{},
	}
	lifecycle := identity("conformance-go-lifecycle", tenant, application)
	claim := must(client.ClaimEffect(ctx, mycelium.ClaimEffectRequest{
		IdentityRequest: lifecycle, Decision: decision,
	}))
	if claim.Disposition != mycelium.ClaimExecute || claim.Handle == nil {
		panic(fmt.Sprintf("expected EXECUTE, got %s", claim.Disposition))
	}
	must(client.RecordEffectBoundary(ctx, claim.Handle, mycelium.BoundaryMaybeCrossed))
	completed := must(client.CompleteEffect(
		ctx, claim.Handle, map[string]any{"client": "go"},
	))
	if completed.EffectState != mycelium.StateCommitted {
		panic(fmt.Sprintf("expected COMMITTED, got %s", completed.EffectState))
	}
	replay := must(client.ClaimEffect(ctx, mycelium.ClaimEffectRequest{
		IdentityRequest: lifecycle, Decision: decision,
	}))
	if replay.Disposition != mycelium.ClaimStoredResult {
		panic(fmt.Sprintf("expected stored result, got %s", replay.Disposition))
	}
	if !reflect.DeepEqual(replay.Result, map[string]any{"client": "go"}) {
		panic(fmt.Sprintf("unexpected replay result: %#v", replay.Result))
	}
	checks = append(checks, "claim-complete-replay")

	manifest := mycelium.ClaimCompositeRequest{
		OperationID: "conformance-go-composite", Definition: "publish-v1",
		Steps: []mycelium.CompositeStep{
			{StepID: "prepare", ToolID: "external_operation"},
			{StepID: "publish", ToolID: "external_operation"},
		},
	}
	if err := client.AssertCompositeCompatible(ctx); err != nil {
		panic(err)
	}
	_, firstParent, err := client.ClaimComposite(ctx, manifest)
	if err != nil {
		panic(err)
	}
	firstIdentity := identity("conformance-go-composite-prepare", tenant, application)
	firstStep := must(client.ClaimCompositeStep(ctx, firstParent, "prepare", mycelium.ClaimEffectRequest{
		IdentityRequest: firstIdentity, Decision: decision,
	}))
	if firstStep.Disposition != mycelium.ClaimExecute || firstStep.Handle == nil {
		panic("composite first step did not execute")
	}
	must(client.BoundaryCompositeStep(ctx, firstParent, "prepare", firstStep.Handle))
	must(client.CompleteCompositeStep(ctx, firstParent, "prepare", firstStep.Handle, map[string]any{"prepared": true}))
	must(client.ReleaseComposite(ctx, firstParent))
	_, resumedParent, err := client.ClaimComposite(ctx, manifest)
	if err != nil {
		panic(err)
	}
	if resumedParent.Fence <= firstParent.Fence {
		panic("composite parent fence did not advance")
	}
	_, err = client.FinishComposite(ctx, firstParent)
	requireProtocolError(err, "STALE_FENCE")
	replayedStep := must(client.ClaimCompositeStep(ctx, resumedParent, "prepare", mycelium.ClaimEffectRequest{
		IdentityRequest: firstIdentity, Decision: decision,
	}))
	if replayedStep.Disposition != mycelium.ClaimStoredResult || !reflect.DeepEqual(replayedStep.Result, map[string]any{"prepared": true}) {
		panic("composite child did not replay stored result")
	}
	must(client.ResolveCompositeStep(ctx, resumedParent, "prepare", firstIdentity))
	secondIdentity := identity("conformance-go-composite-publish", tenant, application)
	secondStep := must(client.ClaimCompositeStep(ctx, resumedParent, "publish", mycelium.ClaimEffectRequest{
		IdentityRequest: secondIdentity, Decision: decision,
	}))
	if secondStep.Disposition != mycelium.ClaimExecute || secondStep.Handle == nil {
		panic("composite second step did not execute")
	}
	must(client.BoundaryCompositeStep(ctx, resumedParent, "publish", secondStep.Handle))
	must(client.CompleteCompositeStep(ctx, resumedParent, "publish", secondStep.Handle, map[string]any{"published": true}))
	if finished := must(client.FinishComposite(ctx, resumedParent)); finished.Status != "COMPLETED" {
		panic("composite did not finish")
	}
	checks = append(checks, "composite-resume-fencing")

	staleIdentity := identity("conformance-go-stale-fence", tenant, application)
	staleClaim := must(client.ClaimEffect(ctx, mycelium.ClaimEffectRequest{
		IdentityRequest: staleIdentity, Decision: decision,
	}))
	if staleClaim.Handle == nil {
		panic("stale-fence claim returned no handle")
	}
	staleClaim.Handle.Fence++
	_, err = client.CompleteEffect(ctx, staleClaim.Handle, map[string]any{"unsafe": true})
	stale := requireProtocolError(err, "STALE_FENCE")
	if !stale.StateMayHaveChanged || !stale.ProviderEffectMayHaveHappened {
		panic("stale-fence error was not conservative")
	}
	checks = append(checks, "stale-fence")

	unknownReply := map[string]any{
		"protocol_version": "v1alpha1",
		"effect_id":        string(expectedEffectID),
		"effect_state":     "ATTEMPTING",
		"terminal_outcome": "in_flight",
		"owner_id":         "future-owner",
		"lease": map[string]any{
			"leased_until":      1,
			"last_heartbeat_at": 1,
		},
		"fence":                  1,
		"provider_boundary":      "not_crossed",
		"provider_operation_ref": nil,
		"result":                 nil,
		"decision":               nil,
		"error":                  nil,
		"disposition":            "FUTURE_DISPOSITION",
	}
	unknownBody := must(json.Marshal(unknownReply))
	unknownHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(unknownBody)),
		}, nil
	})}
	unknownClient := must(mycelium.NewClient(mycelium.ClientOptions{
		BaseURL: baseURL, Token: token, TenantID: tenant, ApplicationID: application,
		HTTPClient: unknownHTTP,
	}))
	_, err = unknownClient.ClaimEffect(ctx, mycelium.ClaimEffectRequest{
		IdentityRequest: identity("conformance-go-unknown", tenant, application),
		Decision:        decision,
	})
	requireProtocolError(err, "UNSUPPORTED_PROTOCOL")
	checks = append(checks, "unknown-value-fail-closed")

	delete(unknownReply, "disposition")
	unknownReply["reconciliation"] = "future-result"
	reconciliationBody := must(json.Marshal(unknownReply))
	reconciliationHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(reconciliationBody)),
		}, nil
	})}
	reconciliationClient := must(mycelium.NewClient(mycelium.ClientOptions{
		BaseURL: baseURL, Token: token, TenantID: tenant, ApplicationID: application,
		HTTPClient: reconciliationHTTP,
	}))
	_, err = reconciliationClient.ReconcileEffect(
		ctx,
		expectedEffectID,
		identity("conformance-go-reconciliation", tenant, application),
	)
	requireProtocolError(err, "INVALID_RESPONSE")
	checks = append(checks, "reconciliation-marker-fail-closed")

	timeoutHTTP := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	timeoutClient := must(mycelium.NewClient(mycelium.ClientOptions{
		BaseURL: baseURL, Token: token, TenantID: tenant, ApplicationID: application,
		HTTPClient: timeoutHTTP, Timeout: 10 * time.Millisecond,
	}))
	_, err = timeoutClient.ClaimEffect(ctx, mycelium.ClaimEffectRequest{
		IdentityRequest: identity("conformance-go-timeout", tenant, application),
		Decision:        decision,
	})
	var transport *mycelium.TransportError
	if !errors.As(err, &transport) {
		panic(fmt.Sprintf("expected TransportError, got %T: %v", err, err))
	}
	if !transport.StateMayHaveChanged || !transport.ProviderEffectMayHaveHappened {
		panic("timeout error was not conservative")
	}
	checks = append(checks, "timeout-is-uncertain")

	result := map[string]any{
		"client": "go", "effect_id": derived.EffectID, "checks": checks,
	}
	raw := must(json.Marshal(result))
	fmt.Println(string(raw))
}
