package main

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

func newRootCommand(out io.Writer) *cobra.Command {
	if out == nil {
		out = os.Stdout
	}
	root := &cobra.Command{
		Use:           "xflow",
		Short:         "XFlow administration CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(out)
	root.SetErr(out)
	root.AddCommand(newDeadLetterCommand(out))
	return root
}

func executeRoot(args ...string) error {
	return executeRootWith(os.Stdout, args...)
}

func executeRootWith(out io.Writer, args ...string) error {
	root := newRootCommand(out)
	root.SetArgs(args)
	return root.Execute()
}
