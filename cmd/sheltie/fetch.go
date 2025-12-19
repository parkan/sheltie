package main

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime/pprof"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage/deferred"
	"github.com/ipld/go-ipld-prime/datamodel"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/parkan/sheltie/pkg/aggregateeventrecorder"
	"github.com/parkan/sheltie/pkg/events"
	"github.com/parkan/sheltie/pkg/sheltie"
	"github.com/parkan/sheltie/pkg/storage"
	"github.com/parkan/sheltie/pkg/types"
	"github.com/urfave/cli/v2"
)

const stdoutFileString string = "-" // a string representing stdout

var fetchFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    "output",
		Aliases: []string{"o"},
		Usage: "the CAR file to write to, may be an existing or a new CAR, " +
			"or use '-' to write to stdout",
		TakesFile: true,
	},
	&cli.BoolFlag{
		Name:    "progress",
		Aliases: []string{"p"},
		Usage:   "print progress output",
	},
	&cli.StringFlag{
		Name: "dag-scope",
		Usage: "describes the fetch behavior at the end of the traversal " +
			"path. Valid values include [all, entity, block].",
		DefaultText: "defaults to all, the entire DAG at the end of the path will " +
			"be fetched",
		Value: "all",
		Action: func(cctx *cli.Context, v string) error {
			switch v {
			case string(trustlessutils.DagScopeAll):
			case string(trustlessutils.DagScopeEntity):
			case string(trustlessutils.DagScopeBlock):
			default:
				return fmt.Errorf("invalid dag-scope parameter, must be of value " +
					"[all, entity, block]")
			}
			return nil
		},
	},
	&cli.StringFlag{
		Name: "entity-bytes",
		Usage: "describes the byte range to consider when selecting the blocks " +
			"from a sharded file. Valid values should be of the form from:to, where " +
			"from and to are byte offsets and to may be '*'",
		DefaultText: "defaults to the entire file, 0:*",
		Action: func(cctx *cli.Context, v string) error {
			if _, err := trustlessutils.ParseByteRange(v); err != nil {
				return fmt.Errorf("invalid entity-bytes parameter, must be of the " +
					"form from:to, where from and to are byte offsets and to may be '*'")
			}
			return nil
		},
	},
	&cli.BoolFlag{
		Name:    "stream",
		Usage:   "stream blocks directly to output; disable to use temp files for deduplication",
		Value:   true,
		Aliases: []string{"s"},
	},
	FlagDelegatedRoutingEndpoint,
	FlagEventRecorderAuth,
	FlagEventRecorderInstanceId,
	FlagEventRecorderUrl,
	FlagVerbose,
	FlagVeryVerbose,
	FlagAllowProviders,
	FlagExcludeProviders,
	FlagTempDir,
	FlagGlobalTimeout,
	FlagSkipBlockVerification,
	&cli.StringFlag{
		Name:  "cpuprofile",
		Usage: "write cpu profile to file",
	},
}

var fetchCmd = &cli.Command{
	Name:   "fetch",
	Usage:  "Fetches content from the IPFS and Filecoin network",
	After:  after,
	Action: fetchAction,
	Flags:  fetchFlags,
}

func fetchAction(cctx *cli.Context) error {
	if cctx.Args().Len() != 1 {
		// "help" becomes a subcommand, clear it to deal with a urfave/cli bug
		// Ref: https://github.com/urfave/cli/blob/v2.25.7/help.go#L253-L255
		cctx.Command.Subcommands = nil
		cli.ShowCommandHelpAndExit(cctx, "fetch", 0)
		return nil
	}

	if cpuprofile := cctx.String("cpuprofile"); cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return fmt.Errorf("could not create CPU profile: %w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("could not start CPU profile: %w", err)
		}
		defer pprof.StopCPUProfile()
	}

	msgWriter := cctx.App.ErrWriter
	dataWriter := cctx.App.Writer

	root, path, scope, byteRange, stream, err := parseCidPath(cctx.Args().Get(0))
	if err != nil {
		return err
	}

	if cctx.IsSet("dag-scope") {
		if scope, err = trustlessutils.ParseDagScope(cctx.String("dag-scope")); err != nil {
			return err
		}
	}

	if cctx.IsSet("entity-bytes") {
		if entityBytes, err := trustlessutils.ParseByteRange(cctx.String("entity-bytes")); err != nil {
			return err
		} else if entityBytes.IsDefault() {
			byteRange = nil
		} else {
			byteRange = &entityBytes
		}
	}

	if cctx.IsSet("stream") {
		stream = cctx.Bool("stream")
	}

	tempDir := cctx.String("tempdir")
	progress := cctx.Bool("progress")

	output := cctx.String("output")
	outfile := fmt.Sprintf("%s.car", root.String())
	if output != "" {
		outfile = output
	}

	sheltieCfg, err := buildSheltieConfigFromCLIContext(cctx, nil)
	if err != nil {
		return err
	}

	eventRecorderURL := cctx.String("event-recorder-url")
	authToken := cctx.String("event-recorder-auth")
	instanceID := cctx.String("event-recorder-instance-id")
	eventRecorderCfg := getEventRecorderConfig(eventRecorderURL, authToken, instanceID)

	err = fetchRun(
		cctx.Context,
		sheltieCfg,
		eventRecorderCfg,
		msgWriter,
		dataWriter,
		root,
		path,
		scope,
		byteRange,
		stream,
		tempDir,
		progress,
		outfile,
	)
	if err != nil {
		return cli.Exit(err, 1)
	}

	return nil
}

func parseCidPath(spec string) (
	root cid.Cid,
	path datamodel.Path,
	scope trustlessutils.DagScope,
	byteRange *trustlessutils.ByteRange,
	stream bool,
	err error,
) {
	scope = trustlessutils.DagScopeAll // default
	stream = true                      // default to streaming mode

	if !strings.HasPrefix(spec, "/ipfs/") {
		cstr := strings.Split(spec, "/")[0]
		path = datamodel.ParsePath(strings.TrimPrefix(spec, cstr))
		if root, err = cid.Parse(cstr); err != nil {
			return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
		}
		return root, path, scope, byteRange, stream, err
	} else {
		specParts := strings.Split(spec, "?")
		spec = specParts[0]

		if root, path, err = trustlesshttp.ParseUrlPath(spec); err != nil {
			return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
		}

		switch len(specParts) {
		case 1:
		case 2:
			query, err := url.ParseQuery(specParts[1])
			if err != nil {
				return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
			}
			scope, err = trustlessutils.ParseDagScope(query.Get("dag-scope"))
			if err != nil {
				return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
			}
			if query.Get("entity-bytes") != "" {
				br, err := trustlessutils.ParseByteRange(query.Get("entity-bytes"))
				if err != nil {
					return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, err
				}
				byteRange = &br
			}
			if query.Has("dups") {
				stream = query.Get("dups") == "y"
			}
		default:
			return cid.Undef, datamodel.Path{}, trustlessutils.DagScopeAll, nil, true, fmt.Errorf("invalid query: %s", spec)
		}

		return root, path, scope, byteRange, stream, nil
	}
}

type progressPrinter struct {
	candidatesFound int
	writer          io.Writer
}

func (pp *progressPrinter) subscriber(event types.RetrievalEvent) {
	switch ret := event.(type) {
	case events.StartedFindingCandidatesEvent:
		fmt.Fprintf(pp.writer, "\rQuerying indexer for %s...\n", ret.RootCid())
	case events.StartedRetrievalEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.ConnectedToProviderEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.FirstByteEvent:
		fmt.Fprintf(pp.writer, "\rRetrieving from [%s] (%s)...\n", events.Identifier(ret), ret.Code())
	case events.CandidatesFoundEvent:
		pp.candidatesFound = len(ret.Candidates())
	case events.CandidatesFilteredEvent:
		if len(fetchProviders) == 0 {
			fmt.Fprintf(pp.writer, "Found %d provider candidate(s) in the indexer:\n", pp.candidatesFound)
		} else {
			fmt.Fprintf(pp.writer, "Using the specified provider(s):\n")
		}
		for _, candidate := range ret.Candidates() {
			fmt.Fprintf(pp.writer, "\r\t%s\n", candidate.Endpoint())
		}
	case events.FailedEvent:
		fmt.Fprintf(pp.writer, "\rRetrieval failure from indexer: %s\n", ret.ErrorMessage())
	case events.FailedRetrievalEvent:
		fmt.Fprintf(pp.writer, "\rRetrieval failure for [%s]: %s\n", events.Identifier(ret), ret.ErrorMessage())
	case events.SucceededEvent:
		// noop, handled at return from Retrieve()
	}
}

type onlyWriter struct {
	w io.Writer
}

func (ow *onlyWriter) Write(p []byte) (n int, err error) {
	return ow.w.Write(p)
}

type fetchRunFunc func(
	ctx context.Context,
	sheltieCfg *sheltie.SheltieConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
	msgWriter io.Writer,
	dataWriter io.Writer,
	rootCid cid.Cid,
	path datamodel.Path,
	dagScope trustlessutils.DagScope,
	entityBytes *trustlessutils.ByteRange,
	stream bool,
	tempDir string,
	progress bool,
	outfile string,
) error

var fetchRun fetchRunFunc = defaultFetchRun

// defaultFetchRun is the handler for the fetch command.
// This abstraction allows the fetch command to be invoked
// programmatically for testing.
func defaultFetchRun(
	ctx context.Context,
	sheltieCfg *sheltie.SheltieConfig,
	eventRecorderCfg *aggregateeventrecorder.EventRecorderConfig,
	msgWriter io.Writer,
	dataWriter io.Writer,
	rootCid cid.Cid,
	path datamodel.Path,
	dagScope trustlessutils.DagScope,
	entityBytes *trustlessutils.ByteRange,
	stream bool,
	tempDir string,
	progress bool,
	outfile string,
) error {
	s, err := sheltie.NewSheltieWithConfig(ctx, sheltieCfg)
	if err != nil {
		return err
	}

	// create and subscribe an event recorder API if an endpoint URL is set
	if eventRecorderCfg.EndpointURL != "" {
		setupSheltieEventRecorder(ctx, eventRecorderCfg, s)
	}

	printPath := path.String()
	if printPath != "" {
		printPath = "/" + printPath
	}
	if len(fetchProviders) == 0 {
		fmt.Fprintf(msgWriter, "Fetching %s", rootCid.String()+printPath)
	} else {
		fmt.Fprintf(msgWriter, "Fetching %s from specified provider(s)", rootCid.String()+printPath)
	}
	if progress {
		fmt.Fprintln(msgWriter)
		pp := &progressPrinter{writer: msgWriter}
		s.RegisterSubscriber(pp.subscriber)
	}

	carOpts := []car.Option{
		car.WriteAsCarV1(true),
		car.StoreIdentityCIDs(false),
		car.UseWholeCIDs(false),
	}

	var carStore types.ReadableWritableStorage
	var carWriter storage.DeferredWriter

	if stream {
		var deferredWriter *deferred.DeferredCarWriter
		streamOpts := []car.Option{
			car.WriteAsCarV1(true),
			car.AllowDuplicatePuts(true),
			car.StoreIdentityCIDs(false),
			car.UseWholeCIDs(true),
		}
		if outfile == stdoutFileString {
			deferredWriter = deferred.NewDeferredCarWriterForStream(&onlyWriter{dataWriter}, []cid.Cid{rootCid}, streamOpts...)
		} else {
			deferredWriter = deferred.NewDeferredCarWriterForPath(outfile, []cid.Cid{rootCid}, streamOpts...)
		}
		carWriter = deferredWriter
		carStore = storage.NewStreamingStore(deferredWriter.BlockWriteOpener())
	} else {
		tempStore := storage.NewDeferredStorageCar(tempDir, rootCid)
		if outfile == stdoutFileString {
			carWriter = deferred.NewDeferredCarWriterForStream(&onlyWriter{dataWriter}, []cid.Cid{rootCid}, carOpts...)
		} else {
			carWriter = deferred.NewDeferredCarWriterForPath(outfile, []cid.Cid{rootCid}, carOpts...)
		}
		carStore = storage.NewCachingTempStore(carWriter.BlockWriteOpener(), tempStore)
	}
	defer carWriter.Close()
	defer func() {
		if closer, ok := carStore.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()

	var blockCount int
	var byteLength uint64
	carWriter.OnPut(func(putBytes int) {
		blockCount++
		byteLength += uint64(putBytes)
		if !progress {
			fmt.Fprint(msgWriter, ".")
		} else {
			fmt.Fprintf(msgWriter, "\rReceived %d blocks / %s...", blockCount, humanize.IBytes(byteLength))
		}
	}, false)

	request, err := types.NewRequestForPath(carStore, rootCid, path.String(), dagScope, entityBytes)
	if err != nil {
		return err
	}
	request.Duplicates = stream

	stats, err := s.Fetch(ctx, request)
	if err != nil {
		fmt.Fprintln(msgWriter)
		return err
	}
	providers := stats.Providers
	if len(providers) == 0 && stats.StorageProviderId.String() != "" {
		providers = []string{stats.StorageProviderId.String()}
	}
	providerStr := "Unknown"
	if len(providers) > 0 {
		providerStr = strings.Join(providers, ", ")
	}
	fmt.Fprintf(msgWriter, "\nFetched [%s] from [%s]:\n"+
		"\tDuration: %s\n"+
		"\t  Blocks: %d\n"+
		"\t   Bytes: %s\n",
		rootCid,
		providerStr,
		stats.Duration,
		blockCount,
		humanize.IBytes(stats.Size),
	)

	return nil
}
