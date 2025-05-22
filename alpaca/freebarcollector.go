package alpaca

import (
	"fmt"
	"net/http"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/rickb777/date/timespan"
	"github.com/rs/zerolog/log"

	md "github.com/jamestunnell/marketdata"
)

type FreeBarCollector struct {
	client *marketdata.Client
}

func NewFreeBarCollector() *FreeBarCollector {
	opts := marketdata.ClientOpts{
		RetryLimit: 2,
		RetryDelay: 20 * time.Second,
	}

	opts.HTTPClient = &http.Client{
		Timeout: 60 * time.Second,
	}

	return &FreeBarCollector{
		client: marketdata.NewClient(opts),
	}
}

func (c *FreeBarCollector) Collect(
	symbol string,
	ts timespan.TimeSpan,
) (md.Bars, error) {
	start, end := ts.Start(), ts.End()

	// the most current end time alpaca allows for free
	latestEndAllowed := time.Now().Add(-(15*time.Minute + time.Second))
	if end.After(latestEndAllowed) {
		end = latestEndAllowed
	}

	alpacaBars, err := c.client.GetBars(symbol, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneMin,
		Start:     start,
		End:       end,
		AsOf:      "-",
	})
	if err != nil {
		return md.Bars{}, fmt.Errorf("failed to get bars from alpaca: %w", err)
	}

	log.Debug().
		Time("start", start).
		Time("end", end).
		Int("count", len(alpacaBars)).
		Msg("collected bars from alpaca")

	bars := make([]*md.Bar, len(alpacaBars))
	for i, alpacaBar := range alpacaBars {
		bar := NewBar(alpacaBar)

		bars[i] = bar
	}

	return bars, nil
}
