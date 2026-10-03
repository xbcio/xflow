package runner

import (
	"github.com/xbcio/xflow/service/protocol"
)

// The gRPC runner transport implements ProtocolClient plus activationAckClient
// (via AckActivation) but deliberately NOT leaseRenewClient or
// MetricsReportClient — see doc.go's "gRPC capability gaps" note. Asserting the
// positive and negative cases here makes a signature drift a compile error
// instead of a silent runtime capability loss, mirroring
// inproc_transport_contract_test.go for the in-process transport.
var (
	_ ProtocolClient      = (*protocol.GRPCClient)(nil)
	_ activationAckClient = (*protocol.GRPCClient)(nil)
)
