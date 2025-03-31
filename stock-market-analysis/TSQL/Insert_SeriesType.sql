USE NSE_BANKING;
GO

INSERT INTO dim.SeriesType (SeriesType, SeriesName)
VALUES
    ('EQ', 'Equity'),
    ('BE', 'Bulk Deal'),
    ('BL', 'Block Deal'),
    ('WT', 'Warrants'),
    ('DP', 'Depository Receipt'),
    ('IP', 'IPOs'),
    ('PC', 'Preferential Issues'),
    ('OT', 'Other'),
    ('FD', 'Fixed Income Securities');
