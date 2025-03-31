USE NSE_BANKING;
GO


INSERT INTO dim.Sector (SectorName, SectorDescription, RegulatoryBody, RiskCategory)
VALUES
    ('Public Sector', 'Banks owned by the government, providing financial services to the public', 'Reserve Bank of India', 'Low'),
    ('Private Sector', 'Banks owned by private entities, offering various banking services', 'Reserve Bank of India', 'Medium'),
    ('Foreign', 'Banks that are based outside the country but operate in India', 'Reserve Bank of India', 'Medium'),
    ('Regional Rural', 'Banks focused on providing financial services to rural areas', 'National Bank for Agriculture and Rural Development', 'High'),
    ('Cooperative', 'Banks that are based on the cooperative principles of mutual help', 'Reserve Bank of India', 'High'),
    ('Development', 'Banks that focus on financing projects for the economic development of sectors', 'Reserve Bank of India', 'Medium'),
    ('Small Finance', 'Banks offering financial inclusion to underserved sections of society', 'Reserve Bank of India', 'Medium'),
    ('Payments', 'Banks offering payment services and financial products but no lending services', 'Reserve Bank of India', 'Low');
