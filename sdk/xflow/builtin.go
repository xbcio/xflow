package xflow

// Blank import so builtin action and trigger nodes self-register their handlers
// via init() whenever the SDK is used, in any mode (local or cluster), and in
// the embedded control plane assembled by NewServer: that process compiles and
// describes workflows against the builtin types, so it must see them
// explicitly rather than through an incidental import elsewhere.
import _ "github.com/xbcio/xflow/node"
