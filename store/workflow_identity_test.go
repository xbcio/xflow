package store

import (
	"errors"
	"strings"
	"testing"
)

// TestValidateExecutionWorkflowIDCountsCharacters pins the unit of the bound:
// the column is VARCHAR(64) and OpenAPI's maxLength counts characters, so 64
// multi-byte runes are accepted and the 65th is refused. A byte-count check
// would reject ids the contract declares valid.
func TestValidateExecutionWorkflowIDCountsCharacters(t *testing.T) {
	if err := ValidateExecutionWorkflowID(strings.Repeat("界", MaxExecutionWorkflowIDLen)); err != nil {
		t.Fatalf("a %d-character id was refused: %v", MaxExecutionWorkflowIDLen, err)
	}
	err := ValidateExecutionWorkflowID(strings.Repeat("界", MaxExecutionWorkflowIDLen+1))
	if !errors.Is(err, ErrInvalidWorkflowID) {
		t.Fatalf("a %d-character id error = %v, want ErrInvalidWorkflowID", MaxExecutionWorkflowIDLen+1, err)
	}
}
