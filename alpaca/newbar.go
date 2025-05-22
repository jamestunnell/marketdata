package alpaca

import (
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	md "github.com/jamestunnell/marketdata"
)

func NewBar(alpacaBar marketdata.Bar) *md.Bar {
	ohlc := &md.OHLC{
		Open:  alpacaBar.Open,
		High:  alpacaBar.High,
		Low:   alpacaBar.Low,
		Close: alpacaBar.Close,
	}

	return &md.Bar{
		Timestamp:  alpacaBar.Timestamp,
		Volume:     alpacaBar.Volume,
		TradeCount: alpacaBar.TradeCount,
		VWAP:       alpacaBar.VWAP,
		OHLC:       ohlc,
	}
}
