package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/parkan/sheltie/pkg/aggregateeventrecorder"
	"github.com/parkan/sheltie/pkg/indexerlookup"
	"github.com/parkan/sheltie/pkg/sheltie"
	"github.com/parkan/sheltie/pkg/net/host"
	"github.com/parkan/sheltie/pkg/retriever"
	"github.com/google/uuid"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/config"
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

func buildLassieConfigFromCLIContext(cctx *cli.Context, lassieOpts []sheltie.SheltieOption, libp2pOpts []config.Option) (*sheltie.SheltieConfig, error) {
	providerTimeout := cctx.Duration("provider-timeout")
	globalTimeout := cctx.Duration("global-timeout")

	lassieOpts = append(lassieOpts, sheltie.WithProviderTimeout(providerTimeout))

	if globalTimeout > 0 {
		lassieOpts = append(lassieOpts, sheltie.WithGlobalTimeout(globalTimeout))
	}

	if len(protocols) > 0 {
		lassieOpts = append(lassieOpts, sheltie.WithProtocols(protocols))
	}

	host, err := host.InitHost(cctx.Context, libp2pOpts)
	if err != nil {
		return nil, err
	}
	lassieOpts = append(lassieOpts, sheltie.WithHost(host))

	if len(fetchProviders) > 0 {
		finderOpt := sheltie.WithCandidateSource(retriever.NewDirectCandidateSource(fetchProviders, retriever.WithLibp2pCandidateDiscovery(host)))
		if cctx.IsSet("delegated-routing-endpoint") {
			logger.Warn("Ignoring delegated-routing-endpoint flag since direct provider is specified")
		}
		lassieOpts = append(lassieOpts, finderOpt)
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
		lassieOpts = append(lassieOpts, sheltie.WithCandidateSource(finder))
		logger.Debug("Using explicit delegated routing endpoint to find candidates", "endpoint", endpoint)
	}

	if len(providerBlockList) > 0 {
		lassieOpts = append(lassieOpts, sheltie.WithProviderBlockList(providerBlockList))
	}

	return sheltie.NewSheltieConfig(lassieOpts...), nil
}

func getEventRecorderConfig(endpointURL string, authToken string, instanceID string) *aggregateeventrecorder.EventRecorderConfig {
	return &aggregateeventrecorder.EventRecorderConfig{
		InstanceID:            instanceID,
		EndpointURL:           endpointURL,
		EndpointAuthorization: authToken,
	}
}

// setupLassieEventRecorder creates and subscribes an EventRecorder if an event recorder URL is given
func setupLassieEventRecorder(
	ctx context.Context,
	cfg *aggregateeventrecorder.EventRecorderConfig,
	lassie *sheltie.Sheltie,
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
		lassie.RegisterSubscriber(eventRecorder.RetrievalEventSubscriber())
		logger.Infow("Reporting retrieval events to event recorder API", "url", cfg.EndpointURL, "instance_id", cfg.InstanceID)
	}
}
