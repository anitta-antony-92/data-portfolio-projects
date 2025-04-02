-- SQL Server implementation
USE NSE_BANKING;


-- Generate base dates (2016-01-01 to 2021-12-31)
-- Then populate all attributes:
with DateCTE as (
	select cast('2016-01-01' as date) as date
	union all
	select dateadd(day, 1, date)
	from DateCTE
	where dateadd(day, 1, date) <= '2021-12-31'
)
INSERT INTO dim.Date (
    Date,
    DayNumber,
    DayName,
    WeekNumber,
    MonthNumber,
    MonthName,
    Quarter,
    [Year],
    IsWeekend,
    IsHoliday,
    IsTradingDay,
    FiscalQuarter,
    FiscalQuarterStartDate,
    FinancialYear
)
select
	date,
	day(Date) as daynumber,
	datename(weekday, date) as dayname,
	datepart(week, date) as weeknumber,
	month(date) as monthnumber,
	datename(month, date) as monthname,
	datepart(quarter, date) as quarter,
	year(date) as year,

	-- Weekend Check (Saturday & Sunday)
    CASE WHEN DATENAME(WEEKDAY, Date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS IsWeekend,

    -- Placeholder for Holidays (default to 0, update later from reference table)
    0 AS IsHoliday,

    -- Trading Day Check (Assume weekends are non-trading days)
    CASE WHEN DATENAME(WEEKDAY, Date) IN ('Saturday', 'Sunday') THEN 0 ELSE 1 END AS IsTradingDay,

	-- Fiscal Quarter (based on April–March Financial Year)
	CASE 
        WHEN MONTH(Date) BETWEEN 4 AND 6 THEN 1
        WHEN MONTH(Date) BETWEEN 7 AND 9 THEN 2
        WHEN MONTH(Date) BETWEEN 10 AND 12 THEN 3
        ELSE 4
    END AS FiscalQuarter,

	-- Fiscal Quarter Start Date (First day of the respective fiscal quarter)
	CASE 
        WHEN MONTH(Date) BETWEEN 4 AND 6 THEN DATEFROMPARTS(YEAR(Date), 4, 1)
        WHEN MONTH(Date) BETWEEN 7 AND 9 THEN DATEFROMPARTS(YEAR(Date), 7, 1)
        WHEN MONTH(Date) BETWEEN 10 AND 12 THEN DATEFROMPARTS(YEAR(Date), 10, 1)
        ELSE DATEFROMPARTS(YEAR(Date)-1, 1, 1)  -- Jan-March belongs to previous fiscal year
    END AS FiscalQuarterStartDate,

    -- Financial Year Calculation (April - March)
    CASE 
        WHEN MONTH(Date) >= 4 THEN CONCAT(YEAR(Date), '-', YEAR(Date)+1)
        ELSE CONCAT(YEAR(Date)-1, '-', YEAR(Date))
    END AS FinancialYear

FROM DateCTE
OPTION (MAXRECURSION 2500);

-- Holiday implementation
update d 
set d.IsHoliday = 1
from dim.date d
join ref.MarketHolidays h on d.Date = h.HolidayDate;

