package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/xbcio/xflow/service/crypto/masterkey"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
)

// supplyKeyInputs are the four at-rest key sources. The previous pair exists
// only for a KEK rotation window (see the credential-key-rotation runbook §3).
type supplyKeyInputs struct {
	currentEnv   string // XFLOW_MASTER_KEY
	currentFile  string // --master-key-file
	previousEnv  string // XFLOW_MASTER_KEY_PREVIOUS
	previousFile string // --master-key-previous-file
}

// loadSupplyAtRest builds the supply at-rest encryptor. A nil result with a
// nil error means no key is configured at all; validateProduction decides
// whether that is fatal. Any key that is supplied but unusable — including a
// previous key that equals the current one — is always an error: continuing
// would either write plaintext or leave a rotation silently unfinishable.
//
// When a previous key is loaded a line naming both key IDs goes to warn, so
// the rotation window is visible in the startup log and is not left open by
// accident. Key IDs are 4-byte fingerprints and safe to print.
func loadSupplyAtRest(in supplyKeyInputs, warn io.Writer) (*supplyenc.AtRest, error) {
	mk, err := masterkey.Load(in.currentEnv, in.currentFile)
	if errors.Is(err, masterkey.ErrNotConfigured) {
		mk, err = nil, nil
	}
	if err != nil {
		return nil, err
	}
	prev, err := masterkey.LoadPrevious(in.previousEnv, in.previousFile, mk)
	switch {
	case errors.Is(err, masterkey.ErrNotConfigured):
		if mk == nil {
			return nil, nil
		}
		return supplyenc.NewAtRest(mk.Derive(supplyenc.SupplyContentInfo)), nil
	case err != nil:
		return nil, err
	}
	a, err := supplyenc.NewAtRestWithPrevious(
		mk.Derive(supplyenc.SupplyContentInfo),
		prev.Derive(supplyenc.SupplyContentInfo),
	)
	if err != nil {
		return nil, err
	}
	if warn != nil {
		fmt.Fprintf(warn, "WARNING supply at-rest KEK rotation window open: sealing under kid=%s, also accepting the previous key; run `xflow supply reseal` then remove XFLOW_MASTER_KEY_PREVIOUS / --master-key-previous-file\n", a.CurrentKeyID())
	}
	return a, nil
}
