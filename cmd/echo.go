package cmd

import (
	"github.com/anvh2/kafka-example/services/echo"
	"github.com/spf13/cobra"
)

var echoCmd = &cobra.Command{
	Use:   "echo",
	Short: "Start echo service",
	Long:  `Start echo service`,
	Run: func(cmd *cobra.Command, args []string) {
		server := echo.NewServer()
		server.Run()
	},
}

func init() {
	RootCmd.AddCommand(echoCmd)
}
