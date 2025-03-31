-- 1. Data Validation Query
USE NSE_BANKING;
SELECT 
    COUNT(*) AS TotalRecords,
    MIN(TradeDate) AS EarliestDate,
    MAX(TradeDate) AS LatestDate,
    COUNT(DISTINCT BankStockSymbol) AS UniqueBankStocks
FROM dbo.NSEBankingStocks;

-- 2. Missing Data Check
SELECT 
    SUM(CASE WHEN OpeningPrice IS NULL THEN 1 ELSE 0 END) AS MissingOpenPrices,
    SUM(CASE WHEN ClosingPrice IS NULL THEN 1 ELSE 0 END) AS MissingClosePrices
FROM NSEBankingStocks;
