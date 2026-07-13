package xflow

// Blank import so builtin action and trigger nodes self-register their handlers
// via init() whenever the SDK is used, in any mode (local or cluster).
import _ "github.com/xbcio/xflow/node"
