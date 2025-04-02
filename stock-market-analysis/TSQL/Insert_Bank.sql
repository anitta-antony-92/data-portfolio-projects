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
VALUES
-- Public Sector Banks (SectorID: 50)
('SBIN', 'State Bank of India', 'INE062A01020', 50, 'LC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('PNB', 'Punjab National Bank', 'INE160A01022', 50, 'MC', '2002-05-13', 'Active', '2002-05-13', NULL, 1),
('BANKBARODA', 'Bank of Baroda', 'INE028A01015', 50, 'LC', '1997-09-20', 'Active', '1997-09-20', NULL, 1),
('CANBK', 'Canara Bank', 'INE476A01014', 50, 'MC', '2002-12-20', 'Active', '2002-12-20', NULL, 1),
('UNIONBANK', 'Union Bank of India', 'INE692A01016', 50, 'MC', '2002-12-20', 'Active', '2002-12-20', NULL, 1),
('BANKINDIA', 'Bank of India', 'INE084A01016', 50, 'MC', '1997-09-19', 'Active', '1997-09-19', NULL, 1),
('IOB', 'Indian Overseas Bank', 'INE565A01014', 50, 'SC', '2000-03-30', 'Active', '2000-03-30', NULL, 1),
('CENTRALBK', 'Central Bank of India', 'INE483A01010', 50, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('UCOBANK', 'UCO Bank', 'INE691A01018', 50, 'SC', '2003-09-30', 'Active', '2003-09-30', NULL, 1),
('INDIANB', 'Indian Bank', 'INE562A01011', 50, 'MC', '1997-09-19', 'Active', '1997-09-19', NULL, 1),
('MAHABANK', 'Bank of Maharashtra', 'INE457A01014', 50, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('PSB', 'Punjab & Sind Bank', 'INE608A01012', 50, 'SC', '2010-12-17', 'Active', '2010-12-17', NULL, 1),
('IDBI', 'IDBI Bank', 'INE008A01015', 50, 'MC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),

-- Private Sector Banks (SectorID: 60)
('HDFCBANK', 'HDFC Bank', 'INE040A01026', 60, 'UC', '1995-11-30', 'Active', '1995-11-30', NULL, 1),
('ICICIBANK', 'ICICI Bank', 'INE090A01021', 60, 'UC', '1998-09-30', 'Active', '1998-09-30', NULL, 1),
('KOTAKBANK', 'Kotak Mahindra Bank', 'INE237A01028', 60, 'LC', '2003-09-30', 'Active', '2003-09-30', NULL, 1),
('AXISBANK', 'Axis Bank', 'INE238A01034', 60, 'LC', '1998-06-30', 'Active', '1998-06-30', NULL, 1),
('INDUSINDBK', 'IndusInd Bank', 'INE095A01012', 60, 'LC', '1998-03-31', 'Active', '1998-03-31', NULL, 1),
('YESBANK', 'Yes Bank', 'INE528G01019', 60, 'MC', '2005-07-12', 'Active', '2005-07-12', NULL, 1),
('FEDERALBNK', 'Federal Bank', 'INE171A01029', 60, 'MC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('BANDHANBNK', 'Bandhan Bank', 'INE545U01014', 60, 'MC', '2018-03-27', 'Active', '2018-03-27', NULL, 1),
('RBLBANK', 'RBL Bank', 'INE976G01028', 60, 'MC', '2016-08-30', 'Active', '2016-08-30', NULL, 1),
('CSBBANK', 'CSB Bank', 'INE679A01013', 60, 'SC', '2019-12-04', 'Active', '2019-12-04', NULL, 1),
('DCBBANK', 'DCB Bank', 'INE503A01015', 60, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('J&KBANK', 'Jammu & Kashmir Bank', 'INE168A01041', 60, 'SC', '1998-09-30', 'Active', '1998-09-30', NULL, 1),
('SOUTHBANK', 'South Indian Bank', 'INE683A01023', 60, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('CUB', 'City Union Bank', 'INE491A01021', 60, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('KARURVYSYA', 'Karur Vysya Bank', 'INE036D01028', 60, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('KTKBANK', 'Karnataka Bank', 'INE614B01018', 60, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('DHANBANK', 'Dhanlaxmi Bank', 'INE680A01011', 60, 'SC', '1995-07-01', 'Active', '1995-07-01', NULL, 1),
('SURYODAY', 'Suryoday Small Finance Bank', 'INE725P01018', 60, 'SC', '2021-03-15', 'Active', '2021-03-15', NULL, 1),
('IDFCBANK', 'IDFC Bank', 'INE092T01019', 60, 'MC', '2015-11-06', 'Merged', '2015-11-06', '2018-12-18', 0),

-- Small Finance Banks (SectorID: 110)
('EQUITAS', 'Equitas Small Finance Bank', 'INE063P01018', 110, 'SC', '2016-11-21', 'Active', '2016-11-21', NULL, 1),
('UJJIVAN', 'Ujjivan Small Finance Bank', 'INE334L01012', 110, 'SC', '2019-12-12', 'Active', '2019-12-12', NULL, 1),
('ESAFSFB', 'ESAF Small Finance Bank', 'INE463M01012', 110, 'SC', '2021-03-15', 'Active', '2021-03-15', NULL, 1),
('AUROPHARMA', 'AU Small Finance Bank', 'INE949L01017', 110, 'MC', '2017-06-27', 'Active', '2017-06-27', NULL, 1),

-- Payment Banks (SectorID: 120)
('PAYTM', 'Paytm Payments Bank', 'INE982J01020', 120, 'SC', '2021-11-18', 'Active', '2021-11-18', NULL, 1);