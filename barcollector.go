package marketdata

import "github.com/rickb777/date/timespan"

type BarCollector interface {
	Collect(symbol string, ts timespan.TimeSpan) (Bars, error)
}
