USE NSE_BANKING;
GO

INSERT INTO [dim].[MarketCap] (MarketCapID, CategoryName, MinValue, MaxValue, CategoryDescription)
VALUES
    ('LC', 'Large Cap', 10000000000.00, NULL, 'Companies with a market capitalization above ₹10,000 crore'),
    ('MC', 'Mid Cap', 5000000000.00, 9999999999.99, 'Companies with a market capitalization between ₹5,000 crore and ₹10,000 crore'),
    ('SC', 'Small Cap', 100000000.00, 4999999999.99, 'Companies with a market capitalization under ₹5,000 crore'),
    ('UC', 'Ultra Cap', 50000000000.00, NULL, 'Companies with a market capitalization above ₹50,000 crore'),
    ('VC', 'Venture Capital', NULL, NULL, 'Early-stage companies with high growth potential but are not yet listed on stock exchanges');
