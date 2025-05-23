package main

import (
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/jamestunnell/marketdata"
	"github.com/jamestunnell/marketdata/alpaca"
	"github.com/rickb777/date"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	todayStr := date.Today().Format(date.RFC3339)
	debug := kingpin.Flag("debug", "Enable debug mode").Bool()
	dir := kingpin.Flag("dir", "Output directory.").Default(".").String()
	sym := kingpin.Flag("sym", "The stock symbol.").Required().String()
	tz := kingpin.Flag("tz", "Time zone location").Default("America/New_York").String()
	start := kingpin.Flag("start", "Start date formatted according to RFC3339.").Required().String()
	end := kingpin.Flag("end", "End date formatted according to RFC3339.").Default(todayStr).String()

	_ = kingpin.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	startDate, err := date.Parse(date.RFC3339, *start)
	if err != nil {
		log.Fatal().Err(err).Str("startDate", *start).Msg("failed to parse start date")
	}

	endDate, err := date.Parse(date.RFC3339, *end)
	if err != nil {
		log.Fatal().Err(err).Str("endDate", *end).Msg("failed to parse end date")
	}

	loc, err := time.LoadLocation(*tz)
	if err != nil {
		log.Fatal().Err(err).Str("timeZone", *tz).Msg("failed to load location")
	}

	cmd := &marketdata.CollectCommand{
		Start:     startDate,
		End:       endDate,
		Dir:       *dir,
		Symbol:    *sym,
		Location:  loc,
		Collector: alpaca.NewFreeBarCollector(loc),
	}

	startTime := time.Now()

	if err := cmd.Init(); err != nil {
		log.Error().Err(err).Msg("failed to initialize command")

		return
	}

	log.Info().Msg("running command")

	if err := cmd.Run(); err != nil {
		log.Error().Err(err).Msg("command failed")
	}

	log.Info().Float64("timeSec", time.Since(startTime).Seconds()).Msg("command complete")
}
