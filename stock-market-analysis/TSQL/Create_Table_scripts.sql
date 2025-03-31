-- Table creation statements
USE NSE_BANKING;

-- Create schemas for organization
CREATE SCHEMA dim;
CREATE SCHEMA fact;
CREATE SCHEMA stg;
CREATE SCHEMA ref;


-- 1. First create reference tables (independent tables)
CREATE TABLE ref.MarketHolidays (
    HolidayDate date NOT NULL,
    HolidayName varchar(100) NOT NULL,
    IsBankHoliday bit NOT NULL DEFAULT 0,
    IsNationalHoliday bit NOT NULL DEFAULT 0,
    CONSTRAINT PK_MarketHolidays PRIMARY KEY (HolidayDate, HolidayName)
);

-- 2. Create dimension tables (in dependency order)
CREATE TABLE dim.SeriesType (
    SeriesType char(2) NOT NULL,
    SeriesName varchar(50) NOT NULL,
    CONSTRAINT PK_SeriesType PRIMARY KEY (SeriesType)
);

CREATE TABLE dim.Sector (
    SectorID int NOT NULL IDENTITY(10,10),
    SectorName varchar(50) NOT NULL,
    SectorDescription varchar(200) NULL,
    RegulatoryBody varchar(50) NULL,
    RiskCategory varchar(20) NULL,
    CONSTRAINT PK_Sector PRIMARY KEY (SectorID),
    CONSTRAINT UQ_SectorName UNIQUE (SectorName)
);

CREATE TABLE dim.MarketCap (
    MarketCapID char(2) NOT NULL,
    CategoryName varchar(30) NOT NULL,
    MinValue decimal(18,2) NULL,
    MaxValue decimal(18,2) NULL,
    CategoryDescription varchar(200) NULL,
    CONSTRAINT PK_MarketCap PRIMARY KEY (MarketCapID),
    CONSTRAINT UQ_CategoryName UNIQUE (CategoryName)
);

-- Date dimension must be created before Bank dimension which references it
CREATE TABLE dim.Date (
    [Date] date NOT NULL,
    DayNumber tinyint NOT NULL,
    DayName varchar(10) NOT NULL,
    WeekNumber tinyint NOT NULL,
    MonthNumber tinyint NOT NULL,
    MonthName varchar(10) NOT NULL,
    Quarter tinyint NOT NULL,
    [Year] smallint NOT NULL,
    IsWeekend bit NOT NULL,
    IsHoliday bit NOT NULL,
    IsTradingDay bit NOT NULL,
    FiscalQuarter tinyint NOT NULL,
    FiscalQuarterStartDate date NOT NULL,
    FinancialYear varchar(7) NOT NULL,
    CONSTRAINT PK_Date PRIMARY KEY ([Date]),
    CONSTRAINT CHK_Date_Valid CHECK ([Date] BETWEEN '2016-01-01' AND '2021-12-31')
);


CREATE TABLE dim.Bank (
    BankID int NOT NULL IDENTITY(100,5),
    BankStockSymbol varchar(10) NOT NULL,
    BankName varchar(100) NOT NULL,
    ISINCode varchar(12) NOT NULL,
    SectorID int NOT NULL,
    MarketCapID char(2) NOT NULL,
    ListingDate date NOT NULL,
    CurrentStatus varchar(20) NOT NULL DEFAULT 'Active',
    StartDate date NOT NULL DEFAULT GETDATE(),
    EndDate date NULL,
    IsActive bit NOT NULL DEFAULT 1,
    CONSTRAINT PK_Bank PRIMARY KEY (BankID),
    CONSTRAINT UQ_BankStockSymbol UNIQUE (BankStockSymbol),
    CONSTRAINT UQ_ISINCode UNIQUE (ISINCode),
    CONSTRAINT FK_Bank_Sector FOREIGN KEY (SectorID) REFERENCES dim.Sector (SectorID),
    CONSTRAINT FK_Bank_MarketCap FOREIGN KEY (MarketCapID) REFERENCES dim.MarketCap (MarketCapID),
    CONSTRAINT CHK_Bank_Dates CHECK (EndDate IS NULL OR EndDate > StartDate)
);


-- 3. Create staging table (loaded directly from CSV)
CREATE TABLE stg.NSEBankingStocks (
    TradeDate date NOT NULL,
    BankStockSymbol varchar(20) NOT NULL,
    SeriesType char(2) NOT NULL,
    PreviousClosePrice decimal(18,2) NULL,
    OpeningPrice decimal(18,2) NULL,
    HighestPrice decimal(18,2) NULL,
    LowestPrice decimal(18,2) NULL,
    LastTradedPrice decimal(18,2) NULL,
    ClosingPrice decimal(18,2) NULL,
    VWAP decimal(18,2) NULL,
    TradedVolume bigint NULL,
    DeliverableVolume bigint NULL,
    PctDeliverable decimal(5,4) NULL,
    TotalTrades int NULL,
    TotalTradedValueTO decimal(30,4) NULL,
    DataLoadTime datetime NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_NSEBankingStocks PRIMARY KEY (TradeDate, BankStockSymbol)
);

CREATE TABLE fact.DailyStockPrices (
    TradeDate date NOT NULL,
    BankID int NOT NULL,
    SeriesType char(2) NOT NULL,
    PreviousClosePrice decimal(18,2) NULL,
    OpeningPrice decimal(18,2) NULL,
    HighestPrice decimal(18,2) NULL,
    LowestPrice decimal(18,2) NULL,
    LastTradedPrice decimal(18,2) NULL,
    ClosingPrice decimal(18,2) NULL,
    VWAP decimal(18,2) NULL,
    TradedVolume bigint NULL,
    DeliverableVolume bigint NULL,
    PctDeliverable decimal(5,4) NULL,
    TotalTrades int NULL,
    TotalTradedValueTO decimal(30,4) NULL,
    CONSTRAINT PK_DailyStockPrices PRIMARY KEY (TradeDate, BankID),
    CONSTRAINT FK_DailyStockPrices_Date FOREIGN KEY (TradeDate) REFERENCES dim.Date ([Date]),
    CONSTRAINT FK_DailyStockPrices_Bank FOREIGN KEY (BankID) REFERENCES dim.Bank (BankID),
    CONSTRAINT FK_DailyStockPrices_SeriesType FOREIGN KEY (SeriesType) REFERENCES dim.SeriesType (SeriesType),
    CONSTRAINT CHK_Prices CHECK (
        LowestPrice <= HighestPrice AND
        OpeningPrice BETWEEN LowestPrice AND HighestPrice AND
        ClosingPrice BETWEEN LowestPrice AND HighestPrice
    )
);

-- Create indexes for performance
CREATE INDEX IX_DailyStockPrices_BankID ON fact.DailyStockPrices(BankID);
CREATE INDEX IX_DailyStockPrices_TradeDate ON fact.DailyStockPrices(TradeDate);
CREATE INDEX IX_Bank_SectorID ON dim.Bank(SectorID);
CREATE INDEX IX_Bank_MarketCapID ON dim.Bank(MarketCapID);
