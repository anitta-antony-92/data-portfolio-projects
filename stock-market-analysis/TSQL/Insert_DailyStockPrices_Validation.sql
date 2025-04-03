Insert into fact.DailyStockPrices
(TradeDate, BankID, SeriesType, PreviousClosePrice, OpeningPrice, HighestPrice, LowestPrice, LastTradedPrice, ClosingPrice, VWAP,
TradedVolume, DeliverableVolume, PctDeliverable, TotalTrades, TotalTradedValueTO)
select 
NSE.TradeDate,
B.BankID,
NSE.EquitySeriesType,
NSE.PreviousClosePrice,
NSE.OpeningPrice,
NSE.HighestPrice,
NSE.LowestPrice,
NSE.LastTradedPrice,
NSE.ClosingPrice,
NSE.VWAP,
NSE.TradedSharesVolume,
NSE.DeliverableSharesVolume,
NSE.PercentDeliverable,
NSE.TotalTradesTransactions,
NSE.TotalTradedValueTO
from dbo.NSEBankingStocks NSE
Join dim.Bank B on B.BankStockSymbol = NSE.BankStockSymbol

-- (37842 rows affected)
-- 41231 actual
-- writing join query to identify rows missed

select NBS.*
from dbo.NSEBankingStocks NBS
join fact.DailyStockPrices DSP on DSP.TradeDate = NBS.TradeDate
-- join dim.Bank B on B.BankStockSymbol = NBS.BankStockSymbol

-- 11,68,626 rows

select NBS.*
from dbo.NSEBankingStocks NBS
LEFT join fact.DailyStockPrices DSP on DSP.TradeDate = NBS.TradeDate

-- 11,68,626 rows

select NBS.*
from dbo.NSEBankingStocks NBS
left join fact.DailyStockPrices DSP on DSP.TradeDate = NBS.TradeDate
where DSP.TradeDate is null;

-- 0 rows, not working

-- using not exists

select * from dbo.NSEBankingStocks N
where not exists (
	select 1 from fact.DailyStockPrices D
	join dim.Bank B on B.BankID= D.BankID
	where D.TradeDate = N.TradeDate
	and B.BankStockSymbol = N.BankStockSymbol)

-- 3389 rows, worked
select 41231 - 37842 as diff 
-- 3389

select distinct BankStockSymbol from dbo.NSEBankingStocks N
where not exists (
	select 1 from fact.DailyStockPrices D
	join dim.Bank B on B.BankID= D.BankID
	where D.TradeDate = N.TradeDate
	and B.BankStockSymbol = N.BankStockSymbol)

/*
BankStockSymbol in dbo.NSEBankingStocks
IDFCFIRSTB -- IDFCBANK
EQUITASBNK -- EQUITAS
HDFC -- HDFCBANK
AUBANK -- was AUROPHARMA in dim.bank, updated to AUBANK
UJJIVANSFB -- UJJIVAN
*/


select * from dim.bank 
order by bankstocksymbol asc

Update dim.bank
set BankStockSymbol = 'AUBANK'
where BankID = 810
and BankStockSymbol = 'AUROPHARMA';


select * from dim.Bank
where BankID = 810
and BankStockSymbol = 'AUROPHARMA';

SELECT * 
FROM dim.Bank
WHERE 
    BankStockSymbol LIKE 'IDFC%' 
    OR BankStockSymbol LIKE 'EQUITA%' 
    OR BankStockSymbol LIKE 'UJJI%' 
    OR BankStockSymbol LIKE 'HDFC%';


update dim.Bank B
set B.Bankstocksymbol =
	case 
		when B.BankStockSymbol = 'HDFCBANK' then 'HDFC'
		when B.BankStockSymbol = 'IDFCBANK' then 'IDFCFIRSTB'
		when B.BankStockSymbol = 'EQUITAS' then 'EQUITASBNK'
		when B.BankStockSymbol = 'UJJIVAN' then 'UJJIVANSFB'
	else B.BankStockSymbol
	end
WHERE 
    BankStockSymbol LIKE 'IDFC%' 
    OR BankStockSymbol LIKE 'EQUITA%' 
    OR BankStockSymbol LIKE 'UJJI%' 
    OR BankStockSymbol LIKE 'HDFC%'; -- wrong not working -- Incorrect syntax near 'B'.

update dim.Bank B
set B.Bankstocksymbol =
	case 
		when B.BankStockSymbol = 'HDFCBANK' then 'HDFC'
		when B.BankStockSymbol = 'IDFCBANK' then 'IDFCFIRSTB'
		when B.BankStockSymbol = 'EQUITAS' then 'EQUITASBNK'
		when B.BankStockSymbol = 'UJJIVAN' then 'UJJIVANSFB'
	else B.BankStockSymbol
	end; -- -- wrong not working -- Incorrect syntax near 'B'.


SELECT * FROM dim.Bank
WHERE BankStockSymbol IN ('HDFCBANK', 'IDFCBANK', 'EQUITAS', 'UJJIVAN');

update dim.Bank B
set B.Bankstocksymbol =
	case 
		when B.BankStockSymbol = 'HDFCBANK' then 'HDFC'
		when B.BankStockSymbol = 'IDFCBANK' then 'IDFCFIRSTB'
		when B.BankStockSymbol = 'EQUITAS' then 'EQUITASBNK'
		when B.BankStockSymbol = 'UJJIVAN' then 'UJJIVANSFB'
	else B.BankStockSymbol
	end
WHERE BankStockSymbol IN ('HDFCBANK', 'IDFCBANK', 'EQUITAS', 'UJJIVAN'); -- wrong not working -- Incorrect syntax near 'B'.
-- The issue is with the alias (B) in your UPDATE statement. T-SQL (SQL Server) does not support table aliases in UPDATE statements.

update dim.Bank 
set Bankstocksymbol =
	case 
		when BankStockSymbol = 'HDFCBANK' then 'HDFC'
		when BankStockSymbol = 'IDFCBANK' then 'IDFCFIRSTB'
		when BankStockSymbol = 'EQUITAS' then 'EQUITASBNK'
		when BankStockSymbol = 'UJJIVAN' then 'UJJIVANSFB'
	else BankStockSymbol
	end; -- 37 rows affected
SELECT * FROM dim.Bank;

-- Now delete rows from dailystockprices and reinsert again

select count(*) FROM [NSE_BANKING].[fact].[DailyStockPrices];
-- 37842 expected is 41231
delete FROM [NSE_BANKING].[fact].[DailyStockPrices];

-- rerun insert statement.
Insert into fact.DailyStockPrices
(TradeDate, BankID, SeriesType, PreviousClosePrice, OpeningPrice, HighestPrice, LowestPrice, LastTradedPrice, ClosingPrice, VWAP,
TradedVolume, DeliverableVolume, PctDeliverable, TotalTrades, TotalTradedValueTO)
select 
NSE.TradeDate,
B.BankID,
NSE.EquitySeriesType,
NSE.PreviousClosePrice,
NSE.OpeningPrice,
NSE.HighestPrice,
NSE.LowestPrice,
NSE.LastTradedPrice,
NSE.ClosingPrice,
NSE.VWAP,
NSE.TradedSharesVolume,
NSE.DeliverableSharesVolume,
NSE.PercentDeliverable,
NSE.TotalTradesTransactions,
NSE.TotalTradedValueTO
from dbo.NSEBankingStocks NSE
Join dim.Bank B on B.BankStockSymbol = NSE.BankStockSymbol

-- (40479 rows affected)

select * from dbo.NSEBankingStocks N
where not exists (
	select 1 from fact.DailyStockPrices D
	join dim.Bank B on B.BankID= D.BankID
	where D.TradeDate = N.TradeDate
	and B.BankStockSymbol = N.BankStockSymbol)

-- 752 rows


select distinct BankStockSymbol from dbo.NSEBankingStocks N
where not exists (
	select 1 from fact.DailyStockPrices D
	join dim.Bank B on B.BankID= D.BankID
	where D.TradeDate = N.TradeDate
	and B.BankStockSymbol = N.BankStockSymbol)

	-- IDFCBANK

	select * from dim.bank where bankstocksymbol = 'IDFCBANK' -- None
	select * from dim.bank where bankstocksymbol like '%IDFC%' -- IDFCFIRSTB
	-- previous update 
	-- when BankStockSymbol = 'IDFCBANK' then 'IDFCFIRSTB'
	-- in Bank it is IDFCFIRSTB
	-- in NSEBANKING ot is IDFCBANK


Update dim.bank
set BankStockSymbol = 'IDFCBANK'
where BankStockSymbol = 'IDFCFIRSTB';

select * from dim.bank
where BankStockSymbol =  'IDFCFIRSTB'

-- rerun insert statement

select count(*) FROM [NSE_BANKING].[fact].[DailyStockPrices];
-- 40479 expected is 41231
delete FROM [NSE_BANKING].[fact].[DailyStockPrices];

Insert into fact.DailyStockPrices
(TradeDate, BankID, SeriesType, PreviousClosePrice, OpeningPrice, HighestPrice, LowestPrice, LastTradedPrice, ClosingPrice, VWAP,
TradedVolume, DeliverableVolume, PctDeliverable, TotalTrades, TotalTradedValueTO)
select 
NSE.TradeDate,
B.BankID,
NSE.EquitySeriesType,
NSE.PreviousClosePrice,
NSE.OpeningPrice,
NSE.HighestPrice,
NSE.LowestPrice,
NSE.LastTradedPrice,
NSE.ClosingPrice,
NSE.VWAP,
NSE.TradedSharesVolume,
NSE.DeliverableSharesVolume,
NSE.PercentDeliverable,
NSE.TotalTradesTransactions,
NSE.TotalTradedValueTO
from dbo.NSEBankingStocks NSE
Join dim.Bank B on B.BankStockSymbol = NSE.BankStockSymbol

-- (40646 rows affected)

-- still some rows are missing

select * from dim.bank
where BankStockSymbol like '%IDF%'

INSERT INTO dim.Bank (
    BankStockSymbol, 
    BankName, 
    ISINCode, 
    SectorID, 
    MarketCapID, 
    ListingDate, 
    CurrentStatus, 
    StartDate, 
    EndDate, 
    IsActive
)
VALUES (
    'IDFCFIRSTB', 
    'IDFC FIRST Bank', 
    'INE092T01018',  -- ISIN Code for IDFC FIRST Bank
    60,              -- Sector ID (Assumed value; please verify)
    'MC',           -- Market Capitalization ID (Assumed value; please verify)
    '2019-01-16',    -- Listing Date on NSE post-merger
    'Active',       -- Current Status post-merger
    '2019-01-16',    -- Start Date of IDFC FIRST Bank operations
    NULL,           -- End Date (NULL as the bank is currently active)
    1                -- IsActive (1 for active)
);

select * from dim.bank where isincode = 'INE092T01019';

update dim.bank
set isincode = 'INE092T01020'
where isincode = 'INE092T01018';

-- delete and reinsert

select count(*) FROM [NSE_BANKING].[fact].[DailyStockPrices];
-- 40646 expected is 41231
delete FROM [NSE_BANKING].[fact].[DailyStockPrices];


Insert into fact.DailyStockPrices
(TradeDate, BankID, SeriesType, PreviousClosePrice, OpeningPrice, HighestPrice, LowestPrice, LastTradedPrice, ClosingPrice, VWAP,
TradedVolume, DeliverableVolume, PctDeliverable, TotalTrades, TotalTradedValueTO)
select 
NSE.TradeDate,
B.BankID,
NSE.EquitySeriesType,
NSE.PreviousClosePrice,
NSE.OpeningPrice,
NSE.HighestPrice,
NSE.LowestPrice,
NSE.LastTradedPrice,
NSE.ClosingPrice,
NSE.VWAP,
NSE.TradedSharesVolume,
NSE.DeliverableSharesVolume,
NSE.PercentDeliverable,
NSE.TotalTradesTransactions,
NSE.TotalTradedValueTO
from dbo.NSEBankingStocks NSE
Join dim.Bank B on B.BankStockSymbol = NSE.BankStockSymbol

-- 41231 rows -- Insert SUCCESS


/* INSERT VALIDATION*/

--- 1. Rows in DailyStockPrices but not in NSEBankingStocks

SELECT A.*
FROM fact.DailyStockPrices A
LEFT JOIN NSEBankingStocks B
    ON A.TradeDate = B.TradeDate
    AND A.BankID = (SELECT BankID FROM dim.Bank WHERE BankStockSymbol = B.BankStockSymbol)
WHERE B.TradeDate IS NULL;  -- Rows that are in DailyStockPrices but not in NSEBankingStocks

-- 0 rows

--- 2. Rows in NSEBankingStocks but not in DailyStockPrices

SELECT B.*
FROM NSEBankingStocks B
LEFT JOIN fact.DailyStockPrices A
    ON A.TradeDate = B.TradeDate
    AND A.BankID = (SELECT BankID FROM dim.Bank WHERE BankStockSymbol = B.BankStockSymbol)
WHERE A.TradeDate IS NULL;  -- Rows that are in NSEBankingStocks but not in DailyStockPrices


--- 3. Rows with Differences in Values (Prices, Volume, etc.)

SELECT A.TradeDate, A.BankID, A.PreviousClosePrice AS DailyPreviousClosePrice, 
       B.PreviousClosePrice AS NSEPreviousClosePrice,
       A.OpeningPrice AS DailyOpeningPrice, B.OpeningPrice AS NSEOpeningPrice,
       A.HighestPrice AS DailyHighestPrice, B.HighestPrice AS NSEHighestPrice,
       A.LowestPrice AS DailyLowestPrice, B.LowestPrice AS NSELowestPrice,
       A.LastTradedPrice AS DailyLastTradedPrice, B.LastTradedPrice AS NSELastTradedPrice,
       A.ClosingPrice AS DailyClosingPrice, B.ClosingPrice AS NSEClosingPrice,
       A.VWAP AS DailyVWAP, B.VWAP AS NSEVWAP,
       A.TradedVolume AS DailyTradedVolume, B.TradedSharesVolume AS NSETradedSharesVolume,
       A.DeliverableVolume AS DailyDeliverableVolume, B.DeliverableSharesVolume AS NSEDeliverableSharesVolume,
       A.PctDeliverable AS DailyPctDeliverable, B.PercentDeliverable AS NSEPercentDeliverable,
       A.TotalTrades AS DailyTotalTrades, B.TotalTradesTransactions AS NSETotalTradesTransactions,
       A.TotalTradedValueTO AS DailyTotalTradedValueTO, B.TotalTradedValueTO AS NSETotalTradedValueTO
FROM fact.DailyStockPrices A
JOIN NSEBankingStocks B
    ON A.TradeDate = B.TradeDate
    AND A.BankID = (SELECT BankID FROM dim.Bank WHERE BankStockSymbol = B.BankStockSymbol)
WHERE A.PreviousClosePrice <> B.PreviousClosePrice
    OR A.OpeningPrice <> B.OpeningPrice
    OR A.HighestPrice <> B.HighestPrice
    OR A.LowestPrice <> B.LowestPrice
    OR A.LastTradedPrice <> B.LastTradedPrice
    OR A.ClosingPrice <> B.ClosingPrice
    OR A.VWAP <> B.VWAP
    OR A.TradedVolume <> B.TradedSharesVolume
    OR A.DeliverableVolume <> B.DeliverableSharesVolume
    OR A.PctDeliverable <> B.PercentDeliverable
    OR A.TotalTrades <> B.TotalTradesTransactions
    OR A.TotalTradedValueTO <> B.TotalTradedValueTO;

-- This query compares all relevant columns between DailyStockPrices and NSEBankingStocks and 
-- finds any differences in values (e.g., prices, volumes, etc.) for rows with matching TradeDate and BankID (based on BankStockSymbol).