package xflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/types"
)

func workflowKey(def *types.WorkflowDef) string {
	return fmt.Sprintf("%s/%s@%s", def.Namespace, def.Name, def.Version)
}

func definitionHash(def *types.WorkflowDef) (string, error) {
	data, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
