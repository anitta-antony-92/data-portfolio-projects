USE NSE_BANKING;
GO

INSERT INTO ref.MarketHolidays (HolidayDate, HolidayName, IsBankHoliday, IsNationalHoliday)
VALUES 
    ('2016-01-26', 'Republic Day', 1, 1),
    ('2016-03-07', 'Mahashivratri', 0, 0),
    ('2016-03-24', 'Holi', 0, 1),
    ('2016-03-25', 'Good Friday', 0, 0),
    ('2016-04-14', 'Dr. Baba Saheb Ambedkar Jayanti', 1, 1),
    ('2016-04-15', 'Ram Navami', 0, 0),
    ('2016-04-19', 'Mahavir Jayanti', 0, 0),
    ('2016-05-01', 'Maharashtra Day', 0, 1),  -- Sunday Holiday
    ('2016-07-06', 'Id-ul-Fitr (Ramzan Id)', 1, 1),
    ('2016-08-15', 'Independence Day', 1, 1),
    ('2016-09-05', 'Ganesh Chaturthi', 0, 0),
    ('2016-09-13', 'Bakri Id', 1, 0),
    ('2016-10-02', 'Mahatma Gandhi Jayanti', 1, 1), -- Sunday Holiday
    ('2016-10-11', 'Dussehra', 0, 1),
    ('2016-10-12', 'Muharram', 0, 0),
    ('2016-10-30', 'Diwali-Laxmi Pujan', 0, 0), -- Sunday, Muhurat Trading Day
    ('2016-10-31', 'Diwali-Balipratipada', 0, 0),
    ('2016-11-14', 'Gurunanak Jayanti', 0, 1),
    ('2016-12-12', 'Id-E-Milad', 0, 0),
    ('2016-12-25', 'Christmas', 1, 1), -- Sunday Holiday
	('2017-01-26', 'Republic Day', 1, 1),
    ('2017-02-24', 'Mahashivratri', 0, 0),
    ('2017-03-13', 'Holi', 0, 1),
    ('2017-04-04', 'Ram Navami', 0, 0),
    ('2017-04-14', 'Dr. Baba Saheb Ambedkar Jayanti', 1, 1),
    ('2017-04-14', 'Good Friday', 0, 0),
    ('2017-05-01', 'Maharashtra Day', 0, 1),
    ('2017-06-26', 'Id-Ul-Fitr (Ramzan Id)', 1, 1),
    ('2017-08-15', 'Independence Day', 1, 1),
    ('2017-08-25', 'Ganesh Chaturthi', 0, 0),
    ('2017-10-02', 'Mahatma Gandhi Jayanti', 1, 1),
    ('2017-10-19', 'Diwali-Laxmi Pujan', 0, 0),
    ('2017-10-20', 'Diwali-Balipratipada', 0, 0),
    ('2017-12-25', 'Christmas', 1, 1),
    ('2017-04-09', 'Mahavir Jayanti', 0, 0), -- Sunday
    ('2017-09-02', 'Bakri Id', 1, 0),         -- Saturday
    ('2017-09-30', 'Dussehra', 0, 1),         -- Saturday
    ('2017-10-01', 'Moharram', 0, 0),         -- Sunday
    ('2017-11-04', 'Gurunanak Jayanti', 0, 1),-- Saturday
	('2018-01-26', 'Republic Day', 1, 1),
    ('2018-02-13', 'Mahashivratri', 0, 0),
    ('2018-03-02', 'Holi', 0, 1),
    ('2018-03-25', 'Ram Navami', 0, 0), -- Sunday
    ('2018-03-29', 'Mahavir Jayanti', 0, 0),
    ('2018-03-30', 'Good Friday', 0, 0),
    ('2018-04-14', 'Dr. Baba Saheb Ambedkar Jayanti', 1, 1), -- Saturday
    ('2018-05-01', 'Maharashtra Day', 0, 1),
    ('2018-06-16', 'Id-ul-Fitr (Ramzan Id)', 1, 1), -- Saturday
    ('2018-08-15', 'Independence Day', 1, 1),
    ('2018-08-22', 'Bakri Id / Eid ul-Adha', 1, 0),
    ('2018-09-13', 'Ganesh Chaturthi', 0, 0),
    ('2018-09-20', 'Muharram', 0, 0),
    ('2018-10-02', 'Mahatma Gandhi Jayanti', 1, 1),
    ('2018-10-18', 'Dussehra', 0, 1),
    ('2018-11-07', 'Diwali-Laxmi Pujan', 0, 0),
    ('2018-11-08', 'Diwali-Balipratipada', 0, 0),
    ('2018-11-23', 'Guru Nanak Jayanti', 0, 1),
    ('2018-12-25', 'Christmas', 1, 1),
	('2019-01-26', 'Republic Day', 1, 1),        -- Saturday
    ('2019-03-04', 'Mahashivratri', 0, 0),       -- Monday
    ('2019-03-21', 'Holi', 0, 1),                -- Thursday
    ('2019-04-13', 'Ram Navami', 0, 0),          -- Saturday
    ('2019-04-14', 'Dr. Baba Saheb Ambedkar Jayanti', 1, 1), -- Sunday
    ('2019-04-17', 'Mahavir Jayanti', 0, 0),     -- Wednesday
    ('2019-04-19', 'Good Friday', 0, 0),         -- Friday
    ('2019-05-01', 'Maharashtra Day', 0, 1),     -- Wednesday
    ('2019-06-05', 'Id-ul-Fitr (Ramzan Id)', 1, 1), -- Wednesday
    ('2019-08-12', 'Bakri Id', 1, 0),            -- Monday
    ('2019-08-15', 'Independence Day', 1, 1),    -- Thursday
    ('2019-09-02', 'Ganesh Chaturthi', 0, 0),    -- Monday
    ('2019-09-10', 'Muharram', 0, 0),            -- Tuesday
    ('2019-10-02', 'Mahatma Gandhi Jayanti', 1, 1), -- Wednesday
    ('2019-10-08', 'Dussehra', 0, 1),            -- Tuesday
    ('2019-10-27', 'Diwali-Laxmi Pujan', 0, 0),  -- Sunday (Muhurat Trading)
    ('2019-10-28', 'Diwali-Balipratipada', 0, 0),-- Monday
    ('2019-11-12', 'Guru Nanak Jayanti', 0, 1),  -- Tuesday
    ('2019-12-25', 'Christmas', 1, 1),           -- Wednesday
	('2020-01-26', 'Republic Day', 1, 1),        -- Sunday
    ('2020-02-21', 'Mahashivratri', 0, 0),       -- Friday
    ('2020-03-10', 'Holi', 0, 1),                -- Tuesday
    ('2020-04-02', 'Ram Navami', 0, 0),          -- Thursday
    ('2020-04-06', 'Mahavir Jayanti', 0, 0),     -- Monday
    ('2020-04-10', 'Good Friday', 0, 0),         -- Friday
    ('2020-04-14', 'Dr. Baba Saheb Ambedkar Jayanti', 1, 1), -- Tuesday
    ('2020-05-01', 'Maharashtra Day', 0, 1),     -- Friday
    ('2020-05-25', 'Id-ul-Fitr (Ramzan Id)', 1, 1), -- Monday
    ('2020-08-01', 'Bakri Id', 1, 0),            -- Saturday
    ('2020-08-15', 'Independence Day', 1, 1),    -- Saturday
    ('2020-08-22', 'Ganesh Chaturthi', 0, 0),    -- Saturday
    ('2020-08-30', 'Muharram', 0, 0),            -- Sunday
    ('2020-10-02', 'Mahatma Gandhi Jayanti', 1, 1), -- Friday
    ('2020-10-25', 'Dussehra', 0, 1),            -- Sunday
    ('2020-11-14', 'Diwali-Laxmi Pujan', 0, 0),  -- Saturday (Muhurat Trading)
    ('2020-11-16', 'Diwali-Balipratipada', 0, 0),-- Monday
    ('2020-11-30', 'Guru Nanak Jayanti', 0, 1),  -- Monday
    ('2020-12-25', 'Christmas', 1, 1),           -- Friday
	('2021-01-26', 'Republic Day', 1, 1),            -- Tuesday
    ('2021-03-11', 'Mahashivratri', 0, 0),           -- Thursday
    ('2021-03-29', 'Holi', 0, 1),                    -- Monday
    ('2021-04-02', 'Good Friday', 0, 0),             -- Friday
    ('2021-04-14', 'Dr. Baba Saheb Ambedkar Jayanti', 1, 1), -- Wednesday
    ('2021-04-21', 'Ram Navami', 0, 0),              -- Wednesday
    ('2021-05-13', 'Id-Ul-Fitr (Ramzan Id)', 1, 1),  -- Thursday
    ('2021-07-21', 'Bakri Id', 1, 0),                -- Wednesday
    ('2021-08-19', 'Muharram', 0, 0),                -- Thursday
    ('2021-09-10', 'Ganesh Chaturthi', 0, 0),        -- Friday
    ('2021-10-02', 'Mahatma Gandhi Jayanti', 1, 1),  -- Saturday
    ('2021-10-15', 'Dussehra', 0, 1),                -- Friday
    ('2021-11-04', 'Diwali-Laxmi Pujan', 0, 0),      -- Thursday
    ('2021-11-05', 'Diwali-Balipratipada', 0, 0),    -- Friday
    ('2021-11-19', 'Guru Nanak Jayanti', 0, 1),      -- Friday
    ('2021-12-25', 'Christmas', 1, 1);               -- Saturday