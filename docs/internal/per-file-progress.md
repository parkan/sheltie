# per-file progress display

## overview

when extracting, show progress bars for each file as they're discovered and downloaded.
the extractor already has callbacks for this (OnFileStart, OnFileProgress, OnFileComplete).

## display concept

```
Extracting bafyxxx to ./output
  video.mp4     [===========         ] 55% 8.2/15 MiB
  readme.txt    [====================] done
  image.png     [=>                  ] 12% 0.5/4 MiB
```

## implementation notes

- files appear when File node is processed (OnFileStart)
- progress updates as chunks are written (OnFileProgress)
- files complete when all chunks arrive (OnFileComplete)
- files can complete out of order
- new files can appear mid-download

## complexity

- multi-line terminal updates require ANSI cursor control or a library like github.com/vbauerster/mpb
- need to track active files and their state
- terminal width handling for progress bars
- scrolling behavior when many files

## current callbacks (already implemented)

```go
ext.OnFileStart = func(path string, bytesWritten int64, totalBytes int64) {}
ext.OnFileProgress = func(path string, bytesWritten int64, totalBytes int64) {}
ext.OnFileComplete = func(path string, bytesWritten int64, totalBytes int64) {}
```

## priority

nice-to-have for large multi-file downloads. defer to post-1.0.
