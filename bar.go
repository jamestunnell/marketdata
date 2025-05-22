package marketdata

import (
	"math"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/rickb777/date"
)

type Bar struct {
	Timestamp  time.Time `json:"t"`
	Volume     uint64    `json:"v"`
	TradeCount uint64    `json:"n"`
	VWAP       float64   `json:"vw"`

	*OHLC
}

type OHLC struct {
	Open  float64 `json:"o"`
	High  float64 `json:"h"`
	Low   float64 `json:"l"`
	Close float64 `json:"c"`
}

const (
	oneHalf   = 1.0 / 2.0
	oneThird  = 1.0 / 3.0
	oneFourth = 1.0 / 4.0
)

func NewBarFromAlpaca(alpacaBar marketdata.Bar) *Bar {
	ohlc := &OHLC{
		Open:  alpacaBar.Open,
		High:  alpacaBar.High,
		Low:   alpacaBar.Low,
		Close: alpacaBar.Close,
	}

	return &Bar{
		Timestamp:  alpacaBar.Timestamp,
		Volume:     alpacaBar.Volume,
		TradeCount: alpacaBar.TradeCount,
		VWAP:       alpacaBar.VWAP,
		OHLC:       ohlc,
	}
}

func (b *Bar) Localize() {
	b.Timestamp = b.Timestamp.Local()
}

func (b *Bar) Date() date.Date {
	yyyy, mm, dd := b.Timestamp.Date()

	return date.New(yyyy, mm, dd)
}

func (b *Bar) HeikinAshi(prev *OHLC) *OHLC {
	return &OHLC{
		Open:  0.5 * (prev.Open + prev.Close),
		Close: 0.25 * (b.Open + b.High + b.Low + b.Close),
		High:  math.Max(math.Max(b.High, b.Open), b.Close),
		Low:   math.Max(math.Max(b.Low, b.Open), b.Close),
	}
}

func (b *Bar) GetVWAP() float64 {
	return b.VWAP
}

func (ohlc *OHLC) Float64s() []float64 {
	return []float64{ohlc.Open, ohlc.High, ohlc.Low, ohlc.Close}
}

func (ohlc *OHLC) GetOpen() float64 {
	return ohlc.Open
}

func (ohlc *OHLC) GetHigh() float64 {
	return ohlc.High
}

func (ohlc *OHLC) GetLow() float64 {
	return ohlc.Low
}

func (ohlc *OHLC) GetClose() float64 {
	return ohlc.Close
}

func (ohlc *OHLC) HL2() float64 {
	return oneHalf * (ohlc.High + ohlc.Low)
}

func (ohlc *OHLC) HLC3() float64 {
	return oneThird * (ohlc.High + ohlc.Low + ohlc.Close)
}

func (ohlc *OHLC) OCC3() float64 {
	return oneThird * (ohlc.Open + ohlc.Close + ohlc.Close)
}

func (ohlc *OHLC) OHLC4() float64 {
	return oneFourth * (ohlc.Open + ohlc.High + ohlc.Low + ohlc.Close)
}

func (ohlc *OHLC) HLCC4() float64 {
	return oneFourth * (ohlc.High + ohlc.Low + ohlc.Close + ohlc.Close)
}
