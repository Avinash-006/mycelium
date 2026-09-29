package mycelium

type ProtocolVersion string
type IdentityVersion string
type CanonicalizationVersion string
type EffectID string
type TenantID string
type ApplicationID string
type AgentID string
type BusinessRequestID string
type ToolID string
type ToolContractVersion string
type OwnerID string
type Fence int64

const ProtocolVersionV1Alpha1 ProtocolVersion = "v1alpha1"

type DecimalValue struct {
	Type    string `json:"$type"`
	Profile string `json:"profile"`
	Value   string `json:"value"`
}
type URLValue struct {
	Type    string `json:"$type"`
	Profile string `json:"profile"`
	Value   string `json:"value"`
}
type JSONValue = any
type Destination map[string]any
type ExecutionScope map[string]any

type IdentityRequest struct {
	ApplicationID           ApplicationID           `json:"application_id"`
	BusinessRequestID       BusinessRequestID       `json:"business_request_id"`
	CanonicalizationVersion CanonicalizationVersion `json:"canonicalization_version"`
	Destination             Destination             `json:"destination"`
	ExecutionScope          ExecutionScope          `json:"execution_scope"`
	IdentityVersion         IdentityVersion         `json:"identity_version"`
	Input                   JSONValue               `json:"input"`
	TenantID                TenantID                `json:"tenant_id"`
	ToolContractVersion     ToolContractVersion     `json:"tool_contract_version"`
	ToolID                  ToolID                  `json:"tool_id"`
	ExpectedEffectID        EffectID                `json:"expected_effect_id,omitempty"`
}
type Decision struct {
	Allowed       bool      `json:"allowed"`
	Verdicts      []Verdict `json:"verdicts"`
	DeniedReasons []string  `json:"denied_reasons"`
}
type Verdict struct {
	Name    string  `json:"name"`
	Allowed bool    `json:"allowed"`
	Reason  *string `json:"reason,omitempty"`
}
type ClaimEffectRequest struct {
	IdentityRequest
	Decision *Decision `json:"decision,omitempty"`
	LeaseTTL *float64  `json:"lease_ttl,omitempty"`
}
type FencedRequest struct {
	OwnerID OwnerID `json:"owner_id"`
	Fence   Fence   `json:"fence"`
}
type RenewLeaseRequest struct {
	IdentityRequest
	FencedRequest
	LeaseTTL *float64 `json:"lease_ttl,omitempty"`
}
type BoundaryState string

const (
	BoundaryNotCrossed   BoundaryState = "not_crossed"
	BoundaryMaybeCrossed BoundaryState = "maybe_crossed"
	BoundaryCrossed      BoundaryState = "crossed"
)

type RecordBoundaryRequest struct {
	IdentityRequest
	FencedRequest
	Boundary BoundaryState `json:"boundary"`
}
type ProviderReference struct {
	ProviderOperationRef string `json:"provider_operation_ref"`
}
type AttachProviderReferenceRequest struct {
	IdentityRequest
	FencedRequest
	ProviderOperationRef string `json:"provider_operation_ref"`
}
type CompleteEffectRequest struct {
	IdentityRequest
	FencedRequest
	Result JSONValue `json:"result,omitempty"`
}
type FailEffectRequest struct {
	IdentityRequest
	FencedRequest
	Boundary BoundaryState `json:"boundary,omitempty"`
}
type ReconcileEffectRequest = IdentityRequest

type Lease struct {
	LeasedUntil     *float64 `json:"leased_until"`
	LastHeartbeatAt *float64 `json:"last_heartbeat_at"`
}
type EffectState string

const (
	StateIntended   EffectState = "INTENDED"
	StateAttempting EffectState = "ATTEMPTING"
	StateCommitted  EffectState = "COMMITTED"
	StateAborted    EffectState = "ABORTED"
	StateUnknown    EffectState = "UNKNOWN"
)

type EffectHandle struct {
	EffectID EffectID `json:"effect_id"`
	OwnerID  OwnerID  `json:"owner_id"`
	Fence    Fence    `json:"fence"`
	identity *IdentityRequest
}
type EffectReply struct {
	ProtocolVersion      ProtocolVersion `json:"protocol_version"`
	EffectID             EffectID        `json:"effect_id"`
	EffectState          EffectState     `json:"effect_state"`
	TerminalOutcome      *string         `json:"terminal_outcome"`
	OwnerID              *OwnerID        `json:"owner_id"`
	Lease                Lease           `json:"lease"`
	Fence                *Fence          `json:"fence"`
	ProviderBoundary     *BoundaryState  `json:"provider_boundary"`
	ProviderOperationRef *string         `json:"provider_operation_ref"`
	Result               JSONValue       `json:"result"`
	Decision             *Decision       `json:"decision"`
	Error                *string         `json:"error"`
	Disposition          string          `json:"disposition,omitempty"`
	Reconciliation       string          `json:"reconciliation,omitempty"`
}
type ClaimDisposition string

const (
	ClaimExecute         ClaimDisposition = "EXECUTE"
	ClaimStoredResult    ClaimDisposition = "RETURN_STORED_RESULT"
	ClaimWaitForOwner    ClaimDisposition = "WAIT_FOR_OWNER"
	ClaimRecordDecision  ClaimDisposition = "RECORD_DECISION"
	ClaimUnknown         ClaimDisposition = "UNKNOWN"
	ClaimDenied          ClaimDisposition = "DENIED"
	ClaimTerminalAborted ClaimDisposition = "TERMINAL_ABORTED"
)

type ClaimReply struct {
	EffectReply
	Disposition ClaimDisposition `json:"disposition"`
	Handle      *EffectHandle    `json:"-"`
}
type CompositeStep struct {
	StepID string `json:"step_id"`
	ToolID string `json:"tool_id"`
}
type ClaimCompositeRequest struct {
	OperationID string          `json:"operation_id"`
	Definition  string          `json:"definition"`
	Steps       []CompositeStep `json:"steps"`
	LeaseTTL    *float64        `json:"lease_ttl,omitempty"`
}
type CompositeReply struct {
	ProtocolVersion          ProtocolVersion           `json:"protocol_version"`
	CompositeProtocolVersion string                    `json:"composite_protocol_version"`
	OperationID              string                    `json:"operation_id"`
	Definition               string                    `json:"definition"`
	ManifestDigest           string                    `json:"manifest_digest"`
	Status                   string                    `json:"status"`
	OwnerID                  *OwnerID                  `json:"owner_id"`
	Fence                    Fence                     `json:"fence"`
	LeaseUntil               *float64                  `json:"lease_until"`
	NextStep                 int                       `json:"next_step"`
	Children                 map[string]map[string]any `json:"children"`
}
type CompositeHandle struct {
	OperationID string
	OwnerID     OwnerID
	Fence       Fence
}
type HealthReply struct {
	Status          string          `json:"status"`
	ProtocolVersion ProtocolVersion `json:"protocol_version"`
}
type CapabilitiesReply struct {
	ProtocolVersion   ProtocolVersion     `json:"protocol_version"`
	IdentityNamespace string              `json:"identity_namespace"`
	Capabilities      []string            `json:"capabilities"`
	Operations        []string            `json:"operations"`
	Extensions        map[string][]string `json:"extensions,omitempty"`
	DevelopmentOnly   bool                `json:"development_only"`
}
type DeriveIdentityReply struct {
	ProtocolVersion   ProtocolVersion `json:"protocol_version"`
	EffectID          EffectID        `json:"effect_id"`
	CanonicalJSON     string          `json:"canonical_json"`
	CanonicalBytes    int             `json:"canonical_bytes"`
	IdentityNamespace string          `json:"identity_namespace"`
}
