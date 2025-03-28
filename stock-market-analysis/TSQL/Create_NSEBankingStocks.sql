USE NSE_BANKING;
GO

CREATE TABLE NSEBankingStocks (
	-- Identification
	TradeDate DATE NOT NULL ,
	BankStockSymbol VARCHAR(20) NOT NULL,
	EquitySeriesType CHAR(2),

	-- Price Series
	PreviousClosePrice DECIMAL(18,2),
	OpeningPrice DECIMAL(18,2),
	HighestPrice DECIMAL(18,2),
	LowestPrice DECIMAL(18,2),
	LastTradedPrice DECIMAL(18,2),
	ClosingPrice DECIMAL(18,2),
	VWAP DECIMAL(18,2),

	-- Volume/Money Flow
	TradedSharesVolume BIGINT,
	DeliverableSharesVolume BIGINT,
	PercentDeliverable DECIMAL(5,4),

	-- Trade Activity
	TotalTradesTransactions INT,
	TotalTradedValueTO DECIMAL(30,4),
	
	-- Metadata
	DataLoadTime DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()

	CONSTRAINT PK_NSEBankingStocks PRIMARY KEY (TradeDate, BankStockSymbol)
);