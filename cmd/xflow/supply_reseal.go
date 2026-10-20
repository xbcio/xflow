package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/service/crypto/masterkey"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// supplyResealOptions holds the flags for `xflow supply reseal`: a one-shot
// maintenance job that opens MySQL directly (like dead-letter reconcile),
// rather than going through the management API, because it needs the at-rest
// key material to build the sqlstore.SupplyResealer the management API has
// no reason to expose.
type supplyResealOptions struct {
	mysqlDSN          string
	masterKeyFile     string
	masterKeyPrevFile string
	dryRun            bool
	batchSize         int
	namespace         string
	out               io.Writer
}

func newSupplyCommand(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "supply",
		Short: "Supply at-rest maintenance commands",
	}
	cmd.AddCommand(newSupplyResealCommand(out))
	cmd.AddCommand(newSupplyKeysCommand(out))
	return cmd
}

func newSupplyResealCommand(out io.Writer) *cobra.Command {
	opts := &supplyResealOptions{out: out}
	cmd := &cobra.Command{
		Use:   "reseal",
		Short: "Rewrite supply rows not sealed under the current at-rest key",
		Long: `reseal opens MySQL directly (like a maintenance job) and rewrites every
supply row that is not sealed under the current master-key-derived at-rest
key: rows sealed under the previous key (a KEK rotation window) and, on a
first-time enablement, pre-encryption plaintext rows.

It requires a current key (XFLOW_MASTER_KEY or --master-key-file); resealing
without one would be a no-op that reports success. It never prints key
material or DSN passwords.

Content is unchanged by design: content_hash, revision and updated_at are
left alone, so downstream consumers that compare content_hash never observe
a reseal as a content change. Each row is locked and resealed in its own
transaction (the same lock PutSupply takes), so this is safe to run during
normal traffic, though a maintenance window is still recommended.

After a real pass (not --dry-run) the command immediately rescans every row
with the CURRENT key alone and adds a "verification" object to the report.
Verification requires every row to open and match its content_hash under that
key, not merely to carry the current kid: kid is self-reported, so a census
alone can call a damaged or wrongly sealed row complete. The command exits
non-zero when any row fails to reseal, still needs resealing, or cannot be
opened under the current key, so a script cannot mistake a partial pass for a
completed rotation: the previous key must stay configured until a pass
reports failed=0 and verification.verified=true.

Requires --mysql-dsn (or XFLOW_MYSQL_DSN).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSupplyReseal(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.mysqlDSN, "mysql-dsn", envOr("XFLOW_MYSQL_DSN", ""),
		"MySQL DSN to reseal (env: XFLOW_MYSQL_DSN)")
	cmd.Flags().StringVar(&opts.masterKeyFile, "master-key-file", "",
		"Path to a 0600 file holding the base64-encoded current master key; XFLOW_MASTER_KEY takes precedence")
	cmd.Flags().StringVar(&opts.masterKeyPrevFile, "master-key-previous-file", "",
		"Path to a 0600 file holding the base64-encoded previous master key (rotation window); XFLOW_MASTER_KEY_PREVIOUS takes precedence")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Classify rows without writing anything")
	cmd.Flags().IntVar(&opts.batchSize, "batch-size", 100, "Row-ID page size for the scan")
	cmd.Flags().StringVar(&opts.namespace, "namespace", "", "Limit the pass to one namespace (empty = every row; a rotation finishes only with an unscoped pass)")
	return cmd
}

// resealKeyMaterial holds the encryptors and safe-to-print key fingerprints a
// reseal pass and its post-apply verification need. The previous key never
// leaves this struct as material: only its fingerprint reaches the report.
type resealKeyMaterial struct {
	// atRest seals under the current key and accepts the previous key for
	// reads, so the pass can open rows written during the rotation window.
	atRest *supplyenc.AtRest
	// current holds the current key alone. Post-apply verification uses it so
	// a row still sealed under the previous key cannot be mistaken for a
	// finished rotation.
	current *supplyenc.AtRest
	// currentKid and previousKid are 4-byte key fingerprints, safe to print.
	// previousKid is empty outside a rotation window.
	currentKid  string
	previousKid string
}

// buildResealKeys loads the master keys with the same env/file precedence rules
// cmd/server uses (env wins over file; see cmd/server/supply_keys.go). It is
// duplicated rather than imported because cmd/server is a separate main
// package this binary cannot import; the two copies are kept small and covered
// by tests on both sides instead of shared through a service/ package.
//
// A current key is required (unlike the server, which tolerates a dev
// deployment with no key at all): resealing without a current key to reseal
// TO is meaningless, so its absence is always an error in this command. op
// names the calling command in that error.
func buildResealKeys(op, masterKeyFile, masterKeyPrevFile string) (*resealKeyMaterial, error) {
	mk, err := masterkey.Load(os.Getenv("XFLOW_MASTER_KEY"), masterKeyFile)
	if err != nil {
		if errors.Is(err, masterkey.ErrNotConfigured) {
			return nil, fmt.Errorf("%s: no current master key configured; set XFLOW_MASTER_KEY or --master-key-file", op)
		}
		return nil, err
	}
	currentDEK := mk.Derive(supplyenc.SupplyContentInfo)
	keys := &resealKeyMaterial{
		current:    supplyenc.NewAtRest(currentDEK),
		currentKid: supplyenc.KeyFromBytes(currentDEK).ID,
	}
	prev, err := masterkey.LoadPrevious(os.Getenv("XFLOW_MASTER_KEY_PREVIOUS"), masterKeyPrevFile, mk)
	switch {
	case errors.Is(err, masterkey.ErrNotConfigured):
		keys.atRest = keys.current
		return keys, nil
	case err != nil:
		return nil, err
	}
	previousDEK := prev.Derive(supplyenc.SupplyContentInfo)
	atRest, err := supplyenc.NewAtRestWithPrevious(currentDEK, previousDEK)
	if err != nil {
		return nil, err
	}
	keys.atRest = atRest
	keys.previousKid = supplyenc.KeyFromBytes(previousDEK).ID
	return keys, nil
}

// buildResealAtRest is the single-encryptor form kept for the reseal pass.
func buildResealAtRest(opts *supplyResealOptions) (*supplyenc.AtRest, error) {
	keys, err := buildResealKeys("supply reseal", opts.masterKeyFile, opts.masterKeyPrevFile)
	if err != nil {
		return nil, err
	}
	return keys.atRest, nil
}

// supplyResealOutput is the CLI report: the pass report, plus the post-apply
// verification when a real (non-dry-run) pass ran. Verification is a separate
// object so a script can gate on verification.verified instead of parsing the
// human-oriented error text.
type supplyResealOutput struct {
	*sqlstore.ResealReport
	Verification *supplyKeyStatusOutput `json:"verification,omitempty"`
}

// supplyResealVerificationError turns a failed post-apply verification into
// the command's non-zero exit. It returns nil when verification passed (or did
// not run).
func supplyResealVerificationError(v *supplyKeyStatusOutput) error {
	if v == nil || v.Verified {
		return nil
	}
	return fmt.Errorf(
		"supply reseal: post-apply verification failed: %d row(s) still need reseal and %d row(s) cannot be read under the current key; keep the previous key configured until a pass reports failed=0 and verified=true",
		v.NeedsReseal, v.Unreadable,
	)
}

func runSupplyReseal(ctx context.Context, opts *supplyResealOptions) error {
	if opts.mysqlDSN == "" {
		return errors.New("--mysql-dsn (or XFLOW_MYSQL_DSN) is required for supply reseal")
	}
	keys, err := buildResealKeys("supply reseal", opts.masterKeyFile, opts.masterKeyPrevFile)
	if err != nil {
		return err
	}

	provider, err := mysqlstore.New(opts.mysqlDSN, mysqlstore.WithSupplyEncryption(keys.atRest))
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}

	report, resealErr := provider.ResealSupplies(ctx, sqlstore.ResealOptions{
		BatchSize: opts.batchSize,
		DryRun:    opts.dryRun,
		Namespace: opts.namespace,
	})
	// Every later use of report (the failures count below, the verification
	// gate) dereferences it. A backend that returns no report is a broken
	// contract, not a quiet success: fail before touching it, and prefer the
	// underlying reseal error when there is one.
	if report == nil {
		if resealErr != nil {
			return fmt.Errorf("supply reseal: %w", resealErr)
		}
		return errors.New("supply reseal: provider returned no reseal report")
	}

	output := &supplyResealOutput{ResealReport: report}
	var verificationErr error
	if !opts.dryRun && resealErr == nil {
		// Verification is a full second read under the current key alone. It
		// closes the window where a lagging replica writes an old-key row
		// after the pass has already scanned past it: a kid census would call
		// such a store clean if the row's kid happened to parse as current,
		// but Open/hash checking cannot be fooled by a self-reported kid.
		var status *sqlstore.SupplyKeyStatus
		status, verificationErr = provider.InspectSupplyKeys(ctx, keys.current, sqlstore.SupplyKeyStatusOptions{
			BatchSize: opts.batchSize,
			Namespace: opts.namespace,
		})
		if status != nil {
			output.Verification = newSupplyKeyStatusOutput(status, keys.previousKid)
		}
	}
	if werr := writeJSONLines(opts.out, output); werr != nil {
		return werr
	}
	if resealErr != nil {
		return fmt.Errorf("supply reseal: %w", resealErr)
	}
	if report.Failed > 0 {
		return fmt.Errorf("supply reseal: %d row(s) failed to reseal; see the failures list in the report above", report.Failed)
	}
	if verificationErr != nil {
		return fmt.Errorf("supply reseal: post-apply verification: %w", verificationErr)
	}
	return supplyResealVerificationError(output.Verification)
}
