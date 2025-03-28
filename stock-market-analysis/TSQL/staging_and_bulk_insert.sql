USE NSE_BANKING;
GO

-- 1. Create staging table with flexible data types
CREATE TABLE #NSEStaging1 (

    [DATE] VARCHAR(20),
    [SYMBOL] VARCHAR(20),
    [SERIES] VARCHAR(10),
    [PREV CLOSE] VARCHAR(50),
    [OPEN] VARCHAR(50),
    [HIGH] VARCHAR(50),
    [LOW] VARCHAR(50),
    [LAST] VARCHAR(50),
    [CLOSE] VARCHAR(50),
    [VWAP] VARCHAR(50),
    [VOLUME] VARCHAR(50),
    [TURNOVER] VARCHAR(50),
    [TRADES] VARCHAR(50),
    [DELIVERABLE VOLUME] VARCHAR(50),
    [%DELIVERBLE] VARCHAR(50)
);
GO

-- 2. Bulk insert into staging (all data as strings)
BULK INSERT #NSEStaging1 
FROM 'C:\Users\Anitta\data-portfolio-projects\stock-market-analysis\data\raw_data\NSE_BANKING_SECTOR.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    KEEPNULLS
);
GO

-- 0.12539999999999998 -- problematic values in %deliverable

-- 4. Insert valid data into main table
INSERT INTO NSEBankingStocks (
    TradeDate,
    BankStockSymbol,
    EquitySeriesType,
    PreviousClosePrice,
    OpeningPrice,
    HighestPrice,
    LowestPrice,
    LastTradedPrice,
    ClosingPrice,
    VWAP,
    TradedSharesVolume,
    DeliverableSharesVolume,
    PercentDeliverable,
    TotalTradesTransactions,
    TotalTradedValueTO
    -- DataLoadTime is automatically populated by DEFAULT
)
SELECT
    -- Date (convert from string to DATE)
    TRY_CONVERT(DATE, [DATE]),
    
    -- Symbol (trim whitespace)
    LTRIM(RTRIM([SYMBOL])),
    
    -- Series (take first 2 characters)
    LEFT(LTRIM(RTRIM([SERIES])), 2),
    
    -- Price values (convert to DECIMAL(18,2))
    TRY_CAST([PREV CLOSE] AS DECIMAL(18,2)),
    TRY_CAST([OPEN] AS DECIMAL(18,2)),
    TRY_CAST([HIGH] AS DECIMAL(18,2)),
    TRY_CAST([LOW] AS DECIMAL(18,2)),
    TRY_CAST([LAST] AS DECIMAL(18,2)),
    TRY_CAST([CLOSE] AS DECIMAL(18,2)),
    TRY_CAST([VWAP] AS DECIMAL(18,2)),
    
    -- Volume values (convert to BIGINT)
    TRY_CAST([VOLUME] AS BIGINT),
    TRY_CAST([DELIVERABLE VOLUME] AS BIGINT),
    
    -- Percentage (round to 4 decimal places and ensure it's between 0-1)
    CASE 
        WHEN TRY_CAST([%DELIVERBLE] AS DECIMAL(10,6)) BETWEEN 0 AND 1 
        THEN ROUND(CAST([%DELIVERBLE] AS DECIMAL(10,6)), 4)
        WHEN TRY_CAST([%DELIVERBLE] AS DECIMAL(10,6)) BETWEEN 0 AND 100 
        THEN ROUND(CAST([%DELIVERBLE] AS DECIMAL(10,6)) / 100.0, 4)
        ELSE NULL
    END,
    
    -- Trade activity
    TRY_CAST([TRADES] AS INT),
    TRY_CAST([TURNOVER] AS DECIMAL(30,4))
FROM 
    #NSEStaging1;
GO




-- Optional: Count inserted rows
DECLARE @InsertedRows INT = @@ROWCOUNT;
PRINT 'Successfully inserted ' + CAST(@InsertedRows AS VARCHAR) + ' rows';

-- Optional: Query any rows that failed validation
SELECT 
    [DATE], [SYMBOL], [%DELIVERBLE] AS OriginalPercent,
    CASE 
        WHEN TRY_CAST([%DELIVERBLE] AS DECIMAL(10,6)) BETWEEN 0 AND 1 
        THEN ROUND(CAST([%DELIVERBLE] AS DECIMAL(10,6)), 4)
        WHEN TRY_CAST([%DELIVERBLE] AS DECIMAL(10,6)) BETWEEN 0 AND 100 
        THEN ROUND(CAST([%DELIVERBLE] AS DECIMAL(10,6)) / 100.0, 4)
        ELSE NULL
    END AS ConvertedPercent
FROM 
    #NSEStaging1;

-- 7. Clean up
DROP TABLE #NSEStaging1;
GO