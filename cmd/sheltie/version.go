package main

import (
	"fmt"

	"github.com/parkan/sheltie/pkg/build"
	"github.com/urfave/cli/v2"
)

var versionCmd = &cli.Command{
	Name:      "version",
	Usage:     "Prints the version and exits",
	UsageText: "sheltie version",
	Flags: []cli.Flag{
		FlagVerbose,
		FlagVeryVerbose,
	},
	Action: versionCommand,
}

func versionCommand(cctx *cli.Context) error {
	fmt.Printf("sheltie version %s\n", build.Version)
	return nil
}
