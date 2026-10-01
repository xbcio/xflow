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

The command exits non-zero when the report's failed count is greater than
zero, so a script cannot mistake a partial pass for a completed rotation:
the previous key must stay configured until a pass (not --dry-run) reports
failed=0.

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

// buildResealAtRest constructs the at-rest encryptor from the same env/file
// precedence rules cmd/server uses (env wins over file; see
// cmd/server/supply_keys.go). It is duplicated rather than imported because
// cmd/server is a separate main package this binary cannot import; the two
// copies are kept small and covered by tests on both sides instead of shared
// through a service/ package.
//
// A current key is required here (unlike the server, which tolerates a dev
// deployment with no key at all): resealing without a current key to reseal
// TO is meaningless, so its absence is always an error in this command.
func buildResealAtRest(opts *supplyResealOptions) (*supplyenc.AtRest, error) {
	mk, err := masterkey.Load(os.Getenv("XFLOW_MASTER_KEY"), opts.masterKeyFile)
	if err != nil {
		if errors.Is(err, masterkey.ErrNotConfigured) {
			return nil, fmt.Errorf("supply reseal: no current master key configured; set XFLOW_MASTER_KEY or --master-key-file")
		}
		return nil, err
	}
	prev, err := masterkey.LoadPrevious(os.Getenv("XFLOW_MASTER_KEY_PREVIOUS"), opts.masterKeyPrevFile, mk)
	switch {
	case errors.Is(err, masterkey.ErrNotConfigured):
		return supplyenc.NewAtRest(mk.Derive(supplyenc.SupplyContentInfo)), nil
	case err != nil:
		return nil, err
	}
	return supplyenc.NewAtRestWithPrevious(
		mk.Derive(supplyenc.SupplyContentInfo),
		prev.Derive(supplyenc.SupplyContentInfo),
	)
}

func runSupplyReseal(ctx context.Context, opts *supplyResealOptions) error {
	if opts.mysqlDSN == "" {
		return errors.New("--mysql-dsn (or XFLOW_MYSQL_DSN) is required for supply reseal")
	}
	atRest, err := buildResealAtRest(opts)
	if err != nil {
		return err
	}

	provider, err := mysqlstore.New(opts.mysqlDSN, mysqlstore.WithSupplyEncryption(atRest))
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}

	report, err := provider.ResealSupplies(ctx, sqlstore.ResealOptions{
		BatchSize: opts.batchSize,
		DryRun:    opts.dryRun,
		Namespace: opts.namespace,
	})
	if report != nil {
		if werr := writeJSONLines(opts.out, report); werr != nil {
			return werr
		}
	}
	if err != nil {
		return fmt.Errorf("supply reseal: %w", err)
	}
	if report.Failed > 0 {
		return fmt.Errorf("supply reseal: %d row(s) failed to reseal; see the failures list in the report above", report.Failed)
	}
	return nil
}
