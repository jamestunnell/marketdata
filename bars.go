package marketdata

import (
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/olivere/ndjson"
	"github.com/rickb777/date/timespan"
)

type Bars []*Bar

func LoadBars(r io.Reader) (Bars, error) {
	reader := ndjson.NewReader(r)
	bars := Bars{}

	for reader.Next() {
		var bar Bar
		if err := reader.Decode(&bar); err != nil {
			return Bars{}, fmt.Errorf("failed to decode bar: %w", err)
		}

		bars = append(bars, &bar)
	}
	if err := reader.Err(); err != nil {
		return Bars{}, fmt.Errorf("failed to read NDJSON: %w", err)
	}

	return bars, nil
}

func StoreBars(w io.Writer, bars Bars) error {
	writer := ndjson.NewWriter(w)
	for i, bar := range bars {
		if err := writer.Encode(bar); err != nil {
			return fmt.Errorf("failed to encode bar %d: %w", i, err)
		}
	}

	return nil
}

func (bars Bars) IndexForward(t time.Time) (int, bool) {
	for i, bar := range bars {
		if bar.Timestamp.Equal(t) {
			return i, true
		}
	}

	return -1, false
}

func (bars Bars) IndexReverse(t time.Time) (int, bool) {
	for i := len(bars) - 1; i >= 0; i-- {
		if bars[i].Timestamp.Equal(t) {
			return i, true
		}
	}

	return -1, false
}

func (bars Bars) BinarySearch(t time.Time) (int, bool) {
	return slices.BinarySearchFunc(bars, t, CompareBarWithTimestamp)
}

func (bars Bars) Last() *Bar {
	count := len(bars)
	if count == 0 {
		return nil
	}

	return bars[count-1]
}

func (bars Bars) LastN(n int) Bars {
	count := len(bars)
	if count == 0 {
		return Bars{}
	}

	return bars[count-n:]
}

func (bars Bars) NextN(index, n int) Bars {
	if index < 0 || index >= len(bars) {
		return Bars{}
	}

	a := index + 1
	b := a + n
	if b > len(bars) {
		b = len(bars)
	}

	return bars[a:b]
}

func (bars Bars) Localize() {
	for _, b := range bars {
		b.Localize()
	}
}

func (bars Bars) Timestamps() []time.Time {
	ts := make([]time.Time, len(bars))

	for i, bar := range bars {
		ts[i] = bar.Timestamp
	}

	return ts
}

func (bars Bars) ClosePrices() []float64 {
	ts := make([]float64, len(bars))

	for i, bar := range bars {
		ts[i] = bar.Close
	}

	return ts
}

func (bars Bars) TimeSpan() timespan.TimeSpan {
	if len(bars) == 0 {
		return timespan.TimeSpan{}
	}

	min := bars[0].Timestamp
	max := bars[0].Timestamp

	for i := 1; i < len(bars); i++ {
		if bars[i].Timestamp.Before(min) {
			min = bars[i].Timestamp
		}

		if bars[i].Timestamp.After(max) {
			max = bars[i].Timestamp
		}
	}

	return timespan.NewTimeSpan(min, max)
}

func CompareBarWithTimestamp(bar *Bar, t time.Time) int {
	return bar.Timestamp.Compare(t)
}
