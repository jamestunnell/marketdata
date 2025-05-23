package alpaca

import (
	"net/http"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/rickb777/date/timespan"

	md "github.com/jamestunnell/marketdata"
)

type FreeBarCollector struct {
	client *marketdata.Client
	loc    *time.Location
}

func NewFreeBarCollector(loc *time.Location) *FreeBarCollector {
	opts := marketdata.ClientOpts{
		RetryLimit: 2,
		RetryDelay: 20 * time.Second,
	}

	opts.HTTPClient = &http.Client{
		Timeout: 60 * time.Second,
	}

	return &FreeBarCollector{
		client: marketdata.NewClient(opts),
		loc:    loc,
	}
}

func (c *FreeBarCollector) Collect(
	symbol string,
	ts timespan.TimeSpan,
) (md.Bars, error) {
	return GetBarsOneMin(c.client, symbol, ts, c.loc)
}
