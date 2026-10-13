package graph

import (
	"fmt"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/types"
)

// Node type and param values this rule reads. They are string literals rather
// than imports for the same reason compile.go's xflow.split rule is: engine/
// must not depend on node/, and Compile is the only gate every submission path
// shares, so the rule has to live here to be enforced everywhere.
const (
	redisTriggerType = "xflow.trigger.redis"
	redisStreamMode  = "stream"
	redisPubSubMode  = "pubsub"
)

// validateTriggerReplicaCardinality rejects a trigger whose kind cannot be
// hosted by more than one replica.
//
// The one such kind today is the Redis Pub/Sub trigger. A Pub/Sub channel has
// no consumer group, so every subscriber receives every message: with N
// replicas the same publish reaches the control plane N times. On the
// entry-seed path the admission key is a hash of channel and payload bucketed
// to the second, which merges the N submissions into one execution — but only
// for the messages a subscriber genuinely cannot tell apart. Two distinct
// publishes of the same payload in the same second collapse into that one key
// as well, so raising the replica count does not buy throughput; it only widens
// the window in which distinct messages are silently merged.
//
// On the embedded path the same count is refused for the older reason: the
// runner-side subscription lock keeps exactly one replica subscribed, so the
// extra replicas would hold an activation they never serve.
//
// This is a compile-time rule rather than a runtime check because it is a
// property of the DEFINITION: an operator who wrote activation_replicas: 3 on a
// Pub/Sub trigger asked for something the platform cannot deliver, and learning
// that at registration is strictly better than learning it from a metric that
// counts the duplicates. Stream mode is unaffected — a consumer group really
// does distribute entries across replicas.
func validateTriggerReplicaCardinality(where, nodeType string, params map[string]any, replicas uint32) error {
	if replicas <= 1 || nodeType != redisTriggerType {
		return nil
	}
	// An unset mode means stream, the same default configFromParams applies.
	mode := cast.ToString(params["mode"])
	if mode == "" {
		mode = redisStreamMode
	}
	if mode != redisPubSubMode {
		return nil
	}
	return fmt.Errorf("%s: %s in %s mode cannot use activation_replicas=%d: "+
		"a Pub/Sub channel has no consumer group, so every replica receives every message "+
		"and the admission keys they derive merge distinct publishes into one execution",
		where, nodeType, redisPubSubMode, replicas)
}

// validateGroupTriggerReplicaCardinality applies validateTriggerReplicaCardinality
// to a group's entry member. A group that hosts a Pub/Sub trigger has the same
// limitation as the standalone node: the group's replicas each subscribe, and
// the entry unit's fire key cannot separate what they receive.
//
// The entry member is identified the way compileOneGroup identifies it — the
// single trigger-kind member — rather than through resolveGroupEntry, whose
// answer depends on the in-edge state the group pass has not built yet when the
// group validators run.
func validateGroupTriggerReplicaCardinality(g *Graph, gd types.GroupDef) error {
	if gd.ActivationReplicas <= 1 {
		return nil
	}
	for _, member := range gd.Members {
		idx, ok := g.index[member]
		if !ok {
			// An unknown member is compileOneGroup's error to report.
			continue
		}
		node := g.nodes[idx]
		if node.Kind != types.NodeKindTrigger {
			continue
		}
		if err := validateTriggerReplicaCardinality(
			fmt.Sprintf("group %q entry member %q", gd.Name, node.Name),
			node.Type, node.Parameters, gd.ActivationReplicas); err != nil {
			return err
		}
	}
	return nil
}
