// MODIFIED: 2025-10-30
// - Renamed application from lassie to sheltie
// - Removed bitswap flags and configuration
// - Updated logging subsystems for sheltie namespace
// MODIFIED: 2025-12-09
// - HTTP-only, removed graphsync

package main

import (
	"os"
	"strings"
	"time"

	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/parkan/sheltie/pkg/heyfil"
	"github.com/parkan/sheltie/pkg/types"
	"github.com/urfave/cli/v2"
)

var (
	defaultTempDirectory     string   = os.TempDir() // use the system default temp dir
	verboseLoggingSubsystems []string = []string{    // verbose logging is enabled for these subsystems when using the verbose or very-verbose flags
		"sheltie/main",
		"sheltie/retriever",
		"sheltie/httpserver",
		"sheltie/indexerlookup",
		"sheltie/heyfil",
		"sheltie/aggregateeventrecorder",
	}
)

const (
	defaultProviderTimeout time.Duration = 20 * time.Second // 20 seconds
)

// FlagVerbose enables verbose mode, which shows info information about
// operations invoked in the CLI.
var FlagVerbose = &cli.BoolFlag{
	Name:    "verbose",
	Aliases: []string{"v"},
	Usage:   "enable verbose mode for logging",
	Action:  setLogLevel("INFO"),
}

// FlagVeryVerbose enables very verbose mode, which shows debug information about
// operations invoked in the CLI.
var FlagVeryVerbose = &cli.BoolFlag{
	Name:    "very-verbose",
	Aliases: []string{"vv"},
	Usage:   "enable very verbose mode for debugging",
	Action:  setLogLevel("DEBUG"),
}

// setLogLevel returns a CLI Action function that sets the
// logging level for the given subsystems to the given level.
// It is used as an action for the verbose and very-verbose flags.
func setLogLevel(level string) func(*cli.Context, bool) error {
	return func(cctx *cli.Context, _ bool) error {
		// don't override logging if set in the environment.
		if os.Getenv("GOLOG_LOG_LEVEL") != "" {
			return nil
		}
		// set the logging level for the given subsystems
		for _, name := range verboseLoggingSubsystems {
			_ = log.SetLogLevel(name, level)
		}
		return nil
	}
}

// FlagEventRecorderAuth asks for and provides the authorization token for
// sending metrics to an event recorder API via a Basic auth Authorization
// HTTP header. Value will formatted as "Basic <value>" if provided.
var FlagEventRecorderAuth = &cli.StringFlag{
	Name:        "event-recorder-auth",
	Usage:       "the authorization token for an event recorder API",
	DefaultText: "no authorization token will be used",
	EnvVars:     []string{"SHELTIE_EVENT_RECORDER_AUTH", "LASSIE_EVENT_RECORDER_AUTH"},
}

// FlagEventRecorderUrl asks for and provides the URL for an event recorder API
// to send metrics to.
var FlagEventRecorderInstanceId = &cli.StringFlag{
	Name:        "event-recorder-instance-id",
	Usage:       "the instance ID to use for an event recorder API request",
	DefaultText: "a random v4 uuid",
	EnvVars:     []string{"SHELTIE_EVENT_RECORDER_INSTANCE_ID", "LASSIE_EVENT_RECORDER_INSTANCE_ID"},
}

// FlagEventRecorderUrl asks for and provides the URL for an event recorder API
// to send metrics to.
var FlagEventRecorderUrl = &cli.StringFlag{
	Name:        "event-recorder-url",
	Usage:       "the url of an event recorder API",
	DefaultText: "no event recorder API will be used",
	EnvVars:     []string{"SHELTIE_EVENT_RECORDER_URL", "LASSIE_EVENT_RECORDER_URL"},
}

var providerBlockList map[peer.ID]bool
var FlagExcludeProviders = &cli.StringFlag{
	Name:        "exclude-providers",
	DefaultText: "All providers allowed",
	Usage:       "Provider peer IDs, separated by a comma. Example: 12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
	EnvVars:     []string{"SHELTIE_EXCLUDE_PROVIDERS", "LASSIE_EXCLUDE_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		// Do nothing if given an empty string
		if v == "" {
			return nil
		}

		providerBlockList = make(map[peer.ID]bool)
		vs := strings.Split(v, ",")
		for _, v := range vs {
			peerID, err := peer.Decode(v)
			if err != nil {
				return err
			}
			providerBlockList[peerID] = true
		}
		return nil
	},
}

var fetchProviders []types.Provider

var FlagAllowProviders = &cli.StringFlag{
	Name:        "providers",
	Aliases:     []string{"provider"},
	DefaultText: "Providers will be discovered automatically",
	Usage: "Comma-separated addresses of providers, to use instead of " +
		"automatic discovery. Accepts full multiaddrs including peer ID, " +
		"multiaddrs without peer ID and url-style addresses for HTTP and " +
		"Filecoin SP f0 actor addresses. Sheltie will attempt to connect to the " +
		"peer(s). Example: " +
		"/ip4/1.2.3.4/tcp/1234/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4,http://ipfs.io,f01234",
	EnvVars: []string{"SHELTIE_ALLOW_PROVIDERS", "LASSIE_ALLOW_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		// Do nothing if given an empty string
		if v == "" {
			return nil
		}

		// in case we have been given filecoin actor addresses we can look them up
		// with heyfil and translate to full multiaddrs, otherwise this is a
		// pass-through
		trans, err := heyfil.Heyfil{TranslateFaddr: true}.TranslateAll(strings.Split(v, ","))
		if err != nil {
			return err
		}
		fetchProviders, err = types.ParseProviderStrings(strings.Join(trans, ","))
		return err
	},
}

var FlagTempDir = &cli.StringFlag{
	Name:        "tempdir",
	Aliases:     []string{"td"},
	Usage:       "directory to store temporary files while downloading",
	Value:       defaultTempDirectory,
	DefaultText: "os temp directory",
	EnvVars:     []string{"SHELTIE_TEMP_DIRECTORY", "LASSIE_TEMP_DIRECTORY"},
}

var FlagGlobalTimeout = &cli.DurationFlag{
	Name:    "global-timeout",
	Aliases: []string{"gt"},
	Usage:   "consider it an error after not completing a retrieval after this amount of time",
	EnvVars: []string{"SHELTIE_GLOBAL_TIMEOUT", "LASSIE_GLOBAL_TIMEOUT"},
}

var FlagProviderTimeout = &cli.DurationFlag{
	Name:    "provider-timeout",
	Aliases: []string{"pt"},
	Usage:   "consider it an error after not receiving a response from a storage provider after this amount of time",
	Value:   defaultProviderTimeout,
	EnvVars: []string{"SHELTIE_PROVIDER_TIMEOUT", "LASSIE_PROVIDER_TIMEOUT"},
}

var FlagDelegatedRoutingEndpoint = &cli.StringFlag{
	Name:        "delegated-routing-endpoint",
	Aliases:     []string{"delegated"},
	DefaultText: "Defaults to https://cid.contact",
	Usage:       "HTTP endpoint of the delegated routing service used to discover providers.",
	EnvVars:     []string{"SHELTIE_DELEGATED_ROUTING_ENDPOINT"},
}

func ResetGlobalFlags() {
	// Reset global variables here so that they are not used
	// in subsequent calls to commands during testing.
	fetchProviders = make([]types.Provider, 0)
	providerBlockList = make(map[peer.ID]bool)
}
