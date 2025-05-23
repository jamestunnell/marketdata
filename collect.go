package marketdata

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/rickb777/date"
	"github.com/rickb777/date/timespan"
	"github.com/rs/zerolog/log"
)

type CollectCommand struct {
	End, Start date.Date
	Dir        string
	Symbol     string
	Location   *time.Location
	Collector  BarCollector
}

var errEmptySymbol = errors.New("symbol is empty")

func verifyDirExists(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("dir '%s' does not exist", dir)
		}

		return fmt.Errorf("failed to stat '%s': %w", dir, err)
	}

	if !info.IsDir() {
		return fmt.Errorf("'%s' is not a dir", dir)
	}

	return nil
}

func (cmd *CollectCommand) Init() error {
	if cmd.Symbol == "" {
		return errEmptySymbol
	}

	if cmd.Start.After(cmd.End) {
		return fmt.Errorf("start date %s is after end date %s", cmd.Start, cmd.End)
	}

	if err := verifyDirExists(cmd.Dir); err != nil {
		return err
	}

	return nil
}

func (cmd *CollectCommand) Run() error {
	tgzPath := path.Join(cmd.Dir, cmd.Symbol+".tar.gz")

	outFile, err := os.Create(tgzPath)
	if err != nil {
		return fmt.Errorf("failed to create tar.gz file %s: %w", tgzPath, err)
	}

	tempDir, err := os.MkdirTemp("", "collect-"+cmd.Symbol+"-*")
	if err != nil {
		return fmt.Errorf("failed to make temp dir: %w", err)
	}

	defer os.RemoveAll(tempDir)

	start := cmd.Start

	// collect whole years
	for cmd.End.Year() > start.Year() {
		end := date.New(start.Year()+1, 1, 1)

		fpath := path.Join(tempDir, fmt.Sprintf("%s-%d.ndjson", cmd.Symbol, start.Year()))

		if err := cmd.collect(fpath, start, end); err != nil {
			return err
		}

		start = end
	}

	// collect what's left
	if start.Before(cmd.End) {
		fpath := path.Join(tempDir, fmt.Sprintf("%s-%d.ndjson", cmd.Symbol, start.Year()))

		if err := cmd.collect(fpath, start, cmd.End); err != nil {
			return err
		}
	}

	if err := cmd.makeTarGzArchive(os.DirFS(tempDir), outFile); err != nil {
		return fmt.Errorf("failed to make tar.gz: %w", err)
	}

	return nil
}

func (cmd *CollectCommand) collect(fpath string, start, end date.Date) error {
	f, err := os.Create(fpath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fpath, err)
	}

	defer f.Close()

	startTime := time.Now()
	ts := timespan.NewTimeSpan(start.In(cmd.Location), end.In(cmd.Location))

	var bars Bars

	if bars, err = cmd.Collector.Collect(cmd.Symbol, ts); err != nil {
		return fmt.Errorf("failed to load bars: %w", err)
	}

	log.Info().
		Str("symbol", cmd.Symbol).
		Float64("timeSec", time.Since(startTime).Seconds()).
		Stringer("start", start).
		Stringer("end", end).
		Msg("collected bars")

	w := bufio.NewWriter(f)

	if err = StoreBars(w, bars); err != nil {
		return fmt.Errorf("failed to store bars in '%s': %w", fpath, err)
	}

	w.Flush()

	log.Info().Str("file", path.Base(fpath)).Msg("stored bars")

	return nil
}

func (cmd *CollectCommand) makeTarGzArchive(root fs.FS, tgz io.Writer) error {
	var tarBuf bytes.Buffer

	tw := tar.NewWriter(&tarBuf)

	if err := tw.AddFS(root); err != nil {
		return fmt.Errorf("failed to add package to archive: %w", err)
	}

	if err := tw.Close(); err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	zw := gzip.NewWriter(tgz)

	// Setting the Header fields is optional.
	zw.Name = cmd.Symbol + " market data"
	zw.ModTime = time.Now()

	// Copy our data to the gzip writer, which compresses it
	if _, err := io.Copy(zw, &tarBuf); err != nil {
		return fmt.Errorf("failed to write gzip data: %w", err)
	}

	if err := zw.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return nil
}
