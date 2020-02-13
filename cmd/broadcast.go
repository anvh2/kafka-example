package cmd

import (
	"github.com/anvh2/kafka-example/services/broadcast"
	"github.com/spf13/cobra"
)

var broadcastCmd = &cobra.Command{
	Use:   "broadcast",
	Short: "Start broadcast service",
	Long:  `Start broadcast service`,
	Run: func(cmd *cobra.Command, args []string) {
		server := broadcast.NewServer()
		server.Run()
	},
}

func init() {
	RootCmd.AddCommand(broadcastCmd)
}
