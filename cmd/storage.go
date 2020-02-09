package cmd

import (
	"github.com/anvh2/kafka-example/services/storage"
	"github.com/spf13/cobra"
)

var storageCmd = &cobra.Command{
	Use:   "storage",
	Short: "Start storage service",
	Long:  `Start storage service`,
	Run: func(cmd *cobra.Command, args []string) {
		server := storage.NewServer()
		server.Run()
	},
}

func init() {
	RootCmd.AddCommand(storageCmd)
}
