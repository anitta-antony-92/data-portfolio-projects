SELECT 
    'Suspicious Prices' AS CheckType,
    COUNT(*) AS ProblemCount
FROM fact.DailyStockPrices
WHERE 
    LowestPrice > HighestPrice OR
    OpeningPrice NOT BETWEEN LowestPrice AND HighestPrice OR
    ClosingPrice NOT BETWEEN LowestPrice AND HighestPrice;

SELECT 
    S.SectorName,
    m.CategoryName AS MarketCap,
    COUNT(DISTINCT f.BankID) AS Banks,
    COUNT(*) AS Records,
    MIN(f.TradeDate) AS FirstDate,
    MAX(f.TradeDate) AS LastDate
FROM fact.DailyStockPrices f
JOIN dim.Bank b ON f.BankID = b.BankID
JOIN dim.MarketCap m ON b.MarketCapID = m.MarketCapID
join dim.Sector s on s.SectorID= b.SectorID
GROUP BY s.SectorName, m.CategoryName
ORDER BY s.SectorName, m.CategoryName;

-- Verify dimension-fact relationship integrity
SELECT 
    'Missing Banks' AS IssueType,
    COUNT(*) AS ProblemCount
FROM dbo.NSEBankingStocks s
LEFT JOIN dim.Bank b ON s.BankStockSymbol = b.BankStockSymbol
WHERE b.BankID IS NULL

UNION ALL

SELECT 
    'Orphaned Fact Records' AS IssueType,
    COUNT(*) AS ProblemCount
FROM fact.DailyStockPrices f
LEFT JOIN dim.Bank b ON f.BankID = b.BankID
WHERE b.BankID IS NULL

UNION ALL

SELECT 
    'Date Coverage Gaps' AS IssueType,
    COUNT(*) AS ProblemCount
FROM dim.Date d
LEFT JOIN fact.DailyStockPrices f ON d.Date = f.TradeDate
WHERE d.Date BETWEEN '2016-01-01' AND '2021-05-28'
AND f.TradeDate IS NULL
AND d.IsTradingDay = 1;




-- Find exact missing dates with details
SELECT 
    d.Date AS MissingDate,
    d.DayName,
    d.IsHoliday,
    (SELECT COUNT(*) 
     FROM fact.DailyStockPrices f 
     WHERE f.TradeDate = d.Date) AS RecordsPresent
FROM dim.Date d
WHERE d.Date BETWEEN '2016-01-01' AND '2021-05-31'
AND d.IsTradingDay = 1
AND NOT EXISTS (
    SELECT 1 
    FROM fact.DailyStockPrices f 
    WHERE f.TradeDate = d.Date
)
ORDER BY d.Date;

SELECT * FROM ref.MarketHolidays
WHERE HolidayDate BETWEEN '2016-01-01' AND '2021-05-31'
ORDER BY HolidayDate;


UPDATE dim.Date
SET IsTradingDay = 0,
    IsHoliday = 1
WHERE Date IN (
    SELECT HolidayDate 
    FROM ref.MarketHolidays
    );


	-- Check for dates with partial data (some banks missing)
SELECT 
    TradeDate,
    COUNT(*) AS BanksReported,
    (SELECT COUNT(*) FROM dim.Bank WHERE IsActive = 1) AS TotalActiveBanks
FROM fact.DailyStockPrices
GROUP BY TradeDate
HAVING COUNT(*) < (SELECT COUNT(*) FROM dim.Bank WHERE IsActive = 1)
ORDER BY TradeDate;

SELECT 
    f.TradeDate,
    COUNT(*) AS BanksReported,
    AVG(f.ClosingPrice) AS AvgClosingPrice,
    SUM(f.TradedVolume) AS TotalVolume -- Should be 0 for these repaired dates
FROM fact.DailyStockPrices f
WHERE f.TradeDate IN ('2019-04-29', '2019-10-21', '2021-05-31')
GROUP BY f.TradeDate;

UPDATE dim.Date
SET IsTradingDay = 0,
    IsHoliday = 0
    WHERE Date IN ('2019-04-29', '2019-10-21');



-- Should return 0 if all gaps are resolved
SELECT COUNT(*) AS RemainingGaps
FROM dim.Date d
WHERE d.Date BETWEEN '2016-01-01' AND '2021-05-28'
AND d.IsTradingDay = 1
AND NOT EXISTS (
    SELECT 1 
    FROM fact.DailyStockPrices f 
    WHERE f.TradeDate = d.Date
);

-- View complete date coverage
SELECT 
    Year,
    COUNT(*) AS TradingDays,
    SUM(CASE WHEN EXISTS (
        SELECT 1 FROM fact.DailyStockPrices f 
        WHERE f.TradeDate = d.Date
    ) THEN 1 ELSE 0 END) AS DaysWithData,
    COUNT(*) - SUM(CASE WHEN EXISTS (
        SELECT 1 FROM fact.DailyStockPrices f 
        WHERE f.TradeDate = d.Date
    ) THEN 1 ELSE 0 END) AS MissingDays
FROM dim.Date d
WHERE d.IsTradingDay = 1
GROUP BY Year
ORDER BY Year;

/*
Msg 130, Level 15, State 1, Line 131
Cannot perform an aggregate function on an expression containing an aggregate or a subquery.
Msg 130, Level 15, State 1, Line 135
Cannot perform an aggregate function on an expression containing an aggregate or a subquery.
*/


-- Corrected version for yearly coverage analysis
SELECT 
    d.Year,
    COUNT(*) AS TradingDays,
    SUM(CASE WHEN f.TradeDate IS NOT NULL THEN 1 ELSE 0 END) AS DaysWithData,
    COUNT(*) - SUM(CASE WHEN f.TradeDate IS NOT NULL THEN 1 ELSE 0 END) AS MissingDays
FROM dim.Date d
LEFT JOIN (
    SELECT DISTINCT TradeDate 
    FROM fact.DailyStockPrices
) f ON d.Date = f.TradeDate
WHERE d.IsTradingDay = 1
GROUP BY d.Year
ORDER BY d.Year;


-- Using OFFSET-FETCH (SQL Server 2012+)
SELECT TOP 2 *
FROM fact.DailyStockPrices
ORDER BY TradeDate DESC;