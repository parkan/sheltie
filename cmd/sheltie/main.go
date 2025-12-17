// MODIFIED: 2025-10-30
// - Renamed application from lassie to sheltie
// - Removed bitswap concurrency configuration
// - Updated to use delegated routing endpoint (renamed from ipni-endpoint)
// MODIFIED: 2025-12-09
// - Removed libp2p host (HTTP-only)

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/ipfs/go-log/v2"
	"github.com/parkan/sheltie/pkg/aggregateeventrecorder"
	"github.com/parkan/sheltie/pkg/indexerlookup"
	"github.com/parkan/sheltie/pkg/retriever"
	"github.com/parkan/sheltie/pkg/sheltie"
	"github.com/urfave/cli/v2"
)

var logger = log.Logger("sheltie/main")

func main() {
	// set up a context that is canceled when a command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

		select {
		case <-interrupt:
			fmt.Println()
			logger.Info("received interrupt signal")
			cancel()
		case <-ctx.Done():
		}

		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	app := &cli.App{
		Name:    "sheltie",
		Usage:   "Sheltie - Utility for retrieving content from the Filecoin network",
		Suggest: true,
		Flags: []cli.Flag{
			FlagVerbose,
			FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			daemonCmd,
			fetchCmd,
			versionCmd,
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		logger.Fatal(err)
	}
}

func after(cctx *cli.Context) error {
	ResetGlobalFlags()
	return nil
}

func buildSheltieConfigFromCLIContext(cctx *cli.Context, sheltieOpts []sheltie.SheltieOption) (*sheltie.SheltieConfig, error) {
	providerTimeout := cctx.Duration("provider-timeout")
	globalTimeout := cctx.Duration("global-timeout")

	sheltieOpts = append(sheltieOpts, sheltie.WithProviderTimeout(providerTimeout))

	if globalTimeout > 0 {
		sheltieOpts = append(sheltieOpts, sheltie.WithGlobalTimeout(globalTimeout))
	}

	if len(fetchProviders) > 0 {
		finderOpt := sheltie.WithCandidateSource(retriever.NewDirectCandidateSource(fetchProviders))
		if cctx.IsSet("delegated-routing-endpoint") {
			logger.Warn("Ignoring delegated-routing-endpoint flag since direct provider is specified")
		}
		sheltieOpts = append(sheltieOpts, finderOpt)
	} else if cctx.IsSet("delegated-routing-endpoint") {
		endpoint := cctx.String("delegated-routing-endpoint")
		endpointUrl, err := url.ParseRequestURI(endpoint)
		if err != nil {
			logger.Errorw("Failed to parse delegated routing endpoint as URL", "err", err)
			return nil, fmt.Errorf("cannot parse given delegated routing endpoint %s as valid URL: %w", endpoint, err)
		}
		finder, err := indexerlookup.NewCandidateSource(indexerlookup.WithHttpEndpoint(endpointUrl))
		if err != nil {
			logger.Errorw("Failed to instantiate delegated routing candidate finder", "err", err)
			return nil, err
		}
		sheltieOpts = append(sheltieOpts, sheltie.WithCandidateSource(finder))
		logger.Debug("Using explicit delegated routing endpoint to find candidates", "endpoint", endpoint)
	}

	if len(providerBlockList) > 0 {
		sheltieOpts = append(sheltieOpts, sheltie.WithProviderBlockList(providerBlockList))
	}

	return sheltie.NewSheltieConfig(sheltieOpts...), nil
}

func getEventRecorderConfig(endpointURL string, authToken string, instanceID string) *aggregateeventrecorder.EventRecorderConfig {
	return &aggregateeventrecorder.EventRecorderConfig{
		InstanceID:            instanceID,
		EndpointURL:           endpointURL,
		EndpointAuthorization: authToken,
	}
}

// setupSheltieEventRecorder creates and subscribes an EventRecorder if an event recorder URL is given
func setupSheltieEventRecorder(
	ctx context.Context,
	cfg *aggregateeventrecorder.EventRecorderConfig,
	s *sheltie.Sheltie,
) {
	if cfg.EndpointURL != "" {
		if cfg.InstanceID == "" {
			uuid, err := uuid.NewRandom()
			if err != nil {
				logger.Warnw("failed to generate default event recorder instance ID UUID, no instance ID will be provided", "err", err)
			}
			cfg.InstanceID = uuid.String() // returns "" if uuid is invalid
		}

		eventRecorder := aggregateeventrecorder.NewAggregateEventRecorder(ctx, *cfg)
		s.RegisterSubscriber(eventRecorder.RetrievalEventSubscriber())
		logger.Infow("Reporting retrieval events to event recorder API", "url", cfg.EndpointURL, "instance_id", cfg.InstanceID)
	}
}
