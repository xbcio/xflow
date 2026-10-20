package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// supplyKeysStatusOptions holds the flags for `xflow supply keys status`.
type supplyKeysStatusOptions struct {
	mysqlDSN          string
	masterKeyFile     string
	masterKeyPrevFile string
	batchSize         int
	namespace         string
	out               io.Writer
}

// supplyKeyStatusOutput is the CLI shape shared by `supply keys status` and
// the reseal post-apply verification. It adds the safe-to-print key
// fingerprints and one machine-readable verdict to the store's classification.
// It never carries key material: key IDs are 4-byte hashes, counts are numbers,
// and issue reasons come from decrypt/parse errors that do not embed content.
type supplyKeyStatusOutput struct {
	*sqlstore.SupplyKeyStatus
	PreviousKeyID string `json:"previous_key_id,omitempty"`
	// Verified is true when every scanned row is sealed under the current key
	// AND opens (content hash included) with it.
	Verified bool `json:"verified"`
}

func newSupplyKeyStatusOutput(status *sqlstore.SupplyKeyStatus, previousKeyID string) *supplyKeyStatusOutput {
	return &supplyKeyStatusOutput{
		SupplyKeyStatus: status,
		PreviousKeyID:   previousKeyID,
		Verified:        status.NeedsReseal == 0 && status.Unreadable == 0,
	}
}

func newSupplyKeysCommand(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "Supply at-rest key inspection commands",
	}
	cmd.AddCommand(newSupplyKeysStatusCommand(out))
	return cmd
}

func newSupplyKeysStatusCommand(out io.Writer) *cobra.Command {
	opts := &supplyKeysStatusOptions{out: out}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report the supply key distribution without writing anything",
		Long: `status opens MySQL directly (like a maintenance job), reads every supply row,
and classifies it by the key ID in its at-rest envelope. It writes nothing and
prints no key material: only key fingerprints (4-byte hashes), row counts, and
per-row namespace/name/reason for rows it cannot read.

The report partitions every scanned row exactly once: by_key_id counts
envelopes whose kid parsed, plaintext counts non-envelopes (left by
deployments that predate at-rest encryption), and unparsable counts
envelope-prefixed values with no usable kid. needs_reseal counts rows the
reseal pass would still rewrite. verified is true only when needs_reseal and
unreadable are both zero, i.e. every row is sealed under the current key and
opens with it.

The scan reads with the configured keyring: with a previous key configured,
rows still sealed under it are readable and reported under its kid; without
one they cannot be opened. Any row whose kid is not held, or that fails to
decrypt or match its content hash, makes the command exit non-zero. An
in-progress rotation reporting needs_reseal > 0 alone does not: that is the
expected state until a reseal pass completes.

Requires --mysql-dsn (or XFLOW_MYSQL_DSN) and a current master key.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSupplyKeysStatus(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.mysqlDSN, "mysql-dsn", envOr("XFLOW_MYSQL_DSN", ""),
		"MySQL DSN to inspect (env: XFLOW_MYSQL_DSN)")
	cmd.Flags().StringVar(&opts.masterKeyFile, "master-key-file", "",
		"Path to a 0600 file holding the base64-encoded current master key; XFLOW_MASTER_KEY takes precedence")
	cmd.Flags().StringVar(&opts.masterKeyPrevFile, "master-key-previous-file", "",
		"Path to a 0600 file holding the base64-encoded previous master key (rotation window); XFLOW_MASTER_KEY_PREVIOUS takes precedence")
	cmd.Flags().IntVar(&opts.batchSize, "batch-size", 100, "Row page size for the scan")
	cmd.Flags().StringVar(&opts.namespace, "namespace", "", "Limit the scan to one namespace (empty = every row)")
	return cmd
}

func runSupplyKeysStatus(ctx context.Context, opts *supplyKeysStatusOptions) error {
	if opts.mysqlDSN == "" {
		return errors.New("--mysql-dsn (or XFLOW_MYSQL_DSN) is required for supply keys status")
	}
	keys, err := buildResealKeys("supply keys status", opts.masterKeyFile, opts.masterKeyPrevFile)
	if err != nil {
		return err
	}

	// Read-only: no at-rest encryption option is configured because nothing is
	// written here. The inspector (with the current+previous keyring) decides
	// what counts as readable.
	provider, err := mysqlstore.New(opts.mysqlDSN)
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}

	status, err := provider.InspectSupplyKeys(ctx, keys.atRest, sqlstore.SupplyKeyStatusOptions{
		BatchSize: opts.batchSize,
		Namespace: opts.namespace,
	})
	if status != nil {
		if werr := writeJSONLines(opts.out, newSupplyKeyStatusOutput(status, keys.previousKid)); werr != nil {
			return werr
		}
	}
	if err != nil {
		return fmt.Errorf("supply keys status: %w", err)
	}
	if status != nil && status.Unreadable > 0 {
		return fmt.Errorf("supply keys status: %d row(s) cannot be read with the configured key(s); see the issues list in the report above", status.Unreadable)
	}
	return nil
}
