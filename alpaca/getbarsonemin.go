package alpaca

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/rickb777/date/timespan"
	"github.com/rs/zerolog/log"

	md "github.com/jamestunnell/marketdata"
)

func GetBarsOneMin(
	client *marketdata.Client,
	sym string,
	ts timespan.TimeSpan,
	loc *time.Location,
) (md.Bars, error) {
	start, end := ts.Start(), ts.End()

	// the most current end time alpaca allows for free
	latestEndAllowed := time.Now().Add(-(15*time.Minute + time.Second))
	if end.After(latestEndAllowed) {
		end = latestEndAllowed
	}

	alpacaBars, err := marketdata.GetBars(sym, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneMin,
		Start:     start,
		End:       end,
		AsOf:      "-",
	})
	if err != nil {
		log.Debug().Err(err).
			Str("start", start.In(loc).Format(time.RFC3339)).
			Str("end", end.In(loc).Format(time.RFC3339)).
			Msg("failed to get bars from alpaca")

		return md.Bars{}, fmt.Errorf("failed to get bars from alpaca: %w", err)
	}

	log.Trace().
		Time("start", start).
		Time("end", end).
		Int("count", len(alpacaBars)).
		Msg("collected bars from alpaca")

	bars := make([]*md.Bar, len(alpacaBars))
	for i, alpacaBar := range alpacaBars {
		bar := NewBar(alpacaBar)

		bar.Timestamp = bar.Timestamp.In(loc)

		bars[i] = bar
	}

	return bars, nil
}
