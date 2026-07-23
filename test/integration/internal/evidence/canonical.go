package evidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// MarshalCanonical returns a stable, canonical JSON representation of v with
// sorted map keys and two-space indentation. It is the form used for artifact
// storage and digest computation.
func MarshalCanonical(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("canonical marshal: %w", err)
	}
	return buf.Bytes(), nil
}

// ManifestDigest returns the SHA-256 canonical digest of the required manifest.
func ManifestDigest(m *Manifest) (string, error) {
	if m == nil {
		return "", fmt.Errorf("manifest digest: nil manifest")
	}
	data, err := MarshalCanonical(m)
	if err != nil {
		return "", fmt.Errorf("manifest digest: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// AtomicFinalize writes the envelope to a temp file in dir and atomically
// renames it to the final artifact path. It also writes a sidecar SHA-256
// digest file computed over the canonical JSON (the digest field is not part
// of the envelope itself).
//
// If verification failed or the envelope has no run ID, it returns an error
// without writing a final artifact.
func AtomicFinalize(envelope *Envelope, dir string) (artifactPath string, digestPath string, err error) {
	if envelope == nil {
		return "", "", fmt.Errorf("atomic finalize: nil envelope")
	}
	if envelope.RunID == "" {
		return "", "", fmt.Errorf("atomic finalize: empty run_id")
	}
	if !envelope.Verification.Passed {
		return "", "", fmt.Errorf("atomic finalize: verification did not pass")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", "", fmt.Errorf("atomic finalize: mkdir %q: %w", dir, err)
	}

	data, err := MarshalCanonical(envelope)
	if err != nil {
		return "", "", err
	}

	base := filepath.Join(dir, "evidence-"+envelope.RunID)
	tmpPath := base + ".tmp"
	finalPath := base + ".json"
	digestPath = base + ".sha256"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return "", "", fmt.Errorf("atomic finalize: write temp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("atomic finalize: rename: %w", err)
	}

	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if err := os.WriteFile(digestPath, []byte(digest+"\n"), 0644); err != nil {
		return "", "", fmt.Errorf("atomic finalize: write digest: %w", err)
	}

	return finalPath, digestPath, nil
}

// AtomicWriteDiagnostic writes a raw/diagnostic envelope without requiring
// verification to pass. It must not overwrite an existing final artifact.
func AtomicWriteDiagnostic(envelope *Envelope, dir string) (string, error) {
	if envelope == nil {
		return "", fmt.Errorf("atomic diagnostic: nil envelope")
	}
	if envelope.RunID == "" {
		return "", fmt.Errorf("atomic diagnostic: empty run_id")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("atomic diagnostic: mkdir %q: %w", dir, err)
	}

	data, err := MarshalCanonical(envelope)
	if err != nil {
		return "", err
	}

	base := filepath.Join(dir, "evidence-"+envelope.RunID+".diagnostic")
	tmpPath := base + ".tmp"
	finalPath := base + ".json"

	if _, err := os.Stat(finalPath); err == nil {
		return "", fmt.Errorf("atomic diagnostic: final artifact already exists for run %s", envelope.RunID)
	}

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return "", fmt.Errorf("atomic diagnostic: write temp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("atomic diagnostic: rename: %w", err)
	}
	return finalPath, nil
}
