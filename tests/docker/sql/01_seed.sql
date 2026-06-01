-- ============================================================
-- StreamXfer Test Data Seeder
-- Target: ~30M rows across 8 tables
--
-- Technique: sys.all_objects CROSS JOIN (≈16M row source) with
--   TOP(N) + CHECKSUM(NEWID()) for fast, truly random column values.
--   Large tables use a WHILE loop with 2M-row batches.
--
-- Row counts:
--   dbo.customers       500,000
--   dbo.products        100,000
--   dbo.orders        2,000,000
--   hr.employees         50,000
--   sales.transactions 2,000,000
--   dbo.order_items   10,000,000   (5 × 2M batches)
--   dbo.events        10,000,000   (5 × 2M batches)
--   dbo.measurements   5,000,000   (3 × 2M + 1M batches)
--
-- Estimated wall time: 10-30 min depending on Docker resources.
-- ============================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

DECLARE @t_total DATETIME2 = GETDATE();
RAISERROR('==============================================', 0, 1) WITH NOWAIT;
RAISERROR('StreamXfer data seeder — target ~30M rows', 0, 1) WITH NOWAIT;
RAISERROR('==============================================', 0, 1) WITH NOWAIT;

-- Truncate all tables for idempotent re-runs
TRUNCATE TABLE dbo.order_items;
TRUNCATE TABLE dbo.events;
TRUNCATE TABLE dbo.measurements;
DELETE FROM dbo.orders;
DELETE FROM dbo.products;
DELETE FROM dbo.customers;
DELETE FROM sales.transactions;
DELETE FROM hr.employees;
RAISERROR('Tables truncated for clean seeding.', 0, 1) WITH NOWAIT;
GO

-- ============================================================
-- 1. dbo.customers  (500,000 rows)
-- ============================================================
DECLARE @t0 DATETIME2 = GETDATE();
RAISERROR('[1/8] dbo.customers (500K)...', 0, 1) WITH NOWAIT;

SELECT TOP(500000)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s13,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s14,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s15,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s16,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s17,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s18
INTO #T
FROM sys.all_objects a, sys.all_objects b
OPTION (MAXDOP 1);

INSERT INTO dbo.customers (
    first_name, last_name, email, phone, birth_date, gender,
    address_line1, city, state_province, country_code, postal_code,
    is_active, credit_limit, loyalty_points, account_balance,
    registration_dt, last_login, notes)
SELECT
    -- first_name
    CHOOSE(s1 % 20 + 1,
        N'James',N'Mary',N'John',N'Patricia',N'Robert',
        N'Jennifer',N'Michael',N'Linda',N'William',N'Barbara',
        N'David',N'Elizabeth',N'Richard',N'Susan',N'Joseph',
        N'Jessica',N'Thomas',N'Sarah',N'Charles',N'Karen'),
    -- last_name
    CHOOSE(s2 % 20 + 1,
        N'Smith',N'Johnson',N'Williams',N'Brown',N'Jones',
        N'Garcia',N'Miller',N'Davis',N'Rodriguez',N'Martinez',
        N'Hernandez',N'Wilson',N'Anderson',N'Taylor',N'Moore',
        N'Jackson',N'Martin',N'Thompson',N'White',N'Harris'),
    -- email: unique (initial + surname-root + rn @ provider)
    SUBSTRING('jmrpbdsakecflnoqtuvwxyz', (rn - 1) % 23 + 1, 1)
        + LOWER(CHOOSE(s3 % 10 + 1,
            'smith','jones','brown','davis','clark',
            'white','moore','taylor','harris','young'))
        + CAST(rn AS VARCHAR(7))
        + '@'
        + CHOOSE(s4 % 6 + 1,
            'gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com','protonmail.com'),
    -- phone
    '+1' + RIGHT('0000000000' + CAST(s5 % 800000000 + 200000000 AS VARCHAR(10)), 10),
    -- birth_date: 20-75 years ago
    CAST(DATEADD(DAY, -(s6 % 20089 + 7305), GETDATE()) AS DATE),
    -- gender: 40% M, 40% F, 20% NULL
    CASE s7 % 5
        WHEN 0 THEN 'M' WHEN 1 THEN 'F'
        WHEN 2 THEN 'M' WHEN 3 THEN 'F'
        ELSE NULL END,
    -- address_line1
    CAST(s8 % 9999 + 1 AS VARCHAR(5)) + ' '
        + CHOOSE(s9 % 10 + 1,
            'Main St','Oak Ave','Maple Dr','Cedar Ln','Park Blvd',
            'Washington Ave','Lake Rd','Hill St','River Dr','Forest Way'),
    -- city
    CHOOSE(s10 % 20 + 1,
        N'New York',N'Los Angeles',N'Chicago',N'Houston',N'Phoenix',
        N'Philadelphia',N'San Antonio',N'San Diego',N'Dallas',N'San Jose',
        N'Austin',N'Jacksonville',N'Fort Worth',N'Columbus',N'Charlotte',
        N'Indianapolis',N'San Francisco',N'Seattle',N'Denver',N'Nashville'),
    -- state_province
    CHOOSE(s11 % 20 + 1,
        'NY','CA','TX','FL','IL','PA','OH','GA','NC','MI',
        'WA','AZ','CO','TN','MA','IN','MO','MD','WI','MN'),
    'US',
    -- postal_code
    RIGHT('00000' + CAST(s12 % 99999 AS VARCHAR(5)), 5),
    -- is_active (90% = 1)
    CAST(CASE WHEN s13 % 10 < 9 THEN 1 ELSE 0 END AS BIT),
    -- credit_limit: $500 – $50,000
    CAST(s14 % 49501 + 500 AS DECIMAL(18,2)),
    -- loyalty_points: 0 – 200,000
    CAST(s15 % 200001 AS BIGINT),
    -- account_balance: –$1,000 – +$50,000
    CAST((s16 % 5100000 - 100000) AS DECIMAL(18,2)) / 100.0,
    -- registration_dt: last 5 years
    DATEADD(SECOND, -(s17 % 157680000), GETDATE()),
    -- last_login: last 90 days (80% non-null)
    CASE WHEN s18 % 10 < 8
         THEN DATEADD(SECOND, -(s18 % 7776000), GETDATE())
         ELSE NULL END,
    -- notes: 20% non-null
    CASE WHEN s1 % 5 = 0
         THEN N'Customer notes: ' + CAST(NEWID() AS NVARCHAR(50))
         ELSE NULL END
FROM #T;

DROP TABLE #T;

DECLARE @rc INT = (SELECT COUNT(*) FROM dbo.customers);
DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [1/8] customers: %d rows, %d sec', 0, 1, @rc, @el) WITH NOWAIT;
GO

-- ============================================================
-- 2. dbo.products  (100,000 rows)
-- ============================================================
DECLARE @t0 DATETIME2 = GETDATE();
RAISERROR('[2/8] dbo.products (100K)...', 0, 1) WITH NOWAIT;

SELECT TOP(100000)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s13,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s14,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s15,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s16,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s17,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s18,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s19,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s20
INTO #T
FROM sys.all_objects a, sys.all_objects b
OPTION (MAXDOP 1);

INSERT INTO dbo.products (
    sku, product_name, description, category, sub_category, brand,
    unit_price, cost_price, weight_kg, stock_qty, reorder_level,
    is_available, rating, review_count, tags, specifications)
SELECT
    'SKU-' + RIGHT('000000' + CAST(rn AS VARCHAR(6)), 6),
    -- product_name
    CHOOSE(s1 % 10 + 1,
        N'Elite',N'Pro',N'Ultra',N'Slim',N'Max',N'Eco',N'Smart',N'Classic',N'Turbo',N'Prime')
        + N' '
        + CHOOSE(s2 % 10 + 1,
            N'Electronics',N'Clothing',N'Food',N'Books',N'Sports',
            N'Home',N'Garden',N'Automotive',N'Health',N'Toys')
        + N' #' + CAST(rn AS NVARCHAR(6)),
    -- description
    N'High-quality product in the '
        + CHOOSE(s3 % 10 + 1,
            N'Electronics',N'Clothing',N'Food',N'Books',N'Sports',
            N'Home',N'Garden',N'Automotive',N'Health',N'Toys')
        + N' category. SKU-' + RIGHT('000000' + CAST(rn AS NVARCHAR(6)), 6)
        + N'. Manufactured to highest standards.',
    -- category
    CHOOSE(s4 % 10 + 1,
        N'Electronics',N'Clothing',N'Food',N'Books',N'Sports',
        N'Home',N'Garden',N'Automotive',N'Health',N'Toys'),
    -- sub_category (80% non-null)
    CASE WHEN s5 % 5 < 4
         THEN CHOOSE(s6 % 10 + 1,
             N'Premium',N'Budget',N'Organic',N'Digital',N'Wireless',
             N'Manual',N'Portable',N'Heavy-Duty',N'Lightweight',N'Compact')
         ELSE NULL END,
    -- brand
    CHOOSE(s7 % 15 + 1,
        N'Apple',N'Samsung',N'Nike',N'Adidas',N'Sony',
        N'LG',N'Dell',N'HP',N'Lenovo',N'Philips',
        N'Bosch',N'Siemens',N'Panasonic',N'Toshiba',N'Sharp'),
    -- unit_price: $1.00 – $999.99
    CAST(s8 % 99900 + 100 AS MONEY) / 100.0,
    -- cost_price: 40–70% of unit_price range
    CAST(s9 % 40000 + 50 AS SMALLMONEY) / 100.0,
    -- weight_kg: 0.01 – 50.00
    CAST(s10 % 4991 + 10 AS REAL) / 100.0,
    -- stock_qty: 0 – 10,000
    s11 % 10001,
    -- reorder_level: 5 – 100
    s12 % 96 + 5,
    -- is_available (95% = 1)
    CAST(CASE WHEN s13 % 20 < 19 THEN 1 ELSE 0 END AS BIT),
    -- rating: 1.00 – 5.00 (80% non-null)
    CASE WHEN s14 % 5 < 4
         THEN CAST((s14 % 401 + 100) / 100.0 AS DECIMAL(3,2))
         ELSE NULL END,
    -- review_count: 0 – 5,000
    s15 % 5001,
    -- tags
    CHOOSE(s16 % 5 + 1,
        N'new,sale',N'featured,hot',N'clearance',N'bestseller,new',N'limited-edition'),
    -- specifications (JSON)
    N'{"weight_g":' + CAST(s17 % 9901 + 100 AS NVARCHAR(5))
        + N',"dims":"' + CAST(s18 % 91 + 10 AS NVARCHAR(3))
        + N'x' + CAST(s19 % 91 + 10 AS NVARCHAR(3))
        + N'x' + CAST(s20 % 91 + 10 AS NVARCHAR(3))
        + N'cm","color":"'
        + CHOOSE(s1 % 10 + 1,
            N'Red',N'Blue',N'Green',N'Black',N'White',
            N'Silver',N'Gold',N'Gray',N'Brown',N'Purple')
        + N'","warranty_months":' + CAST(s2 % 25 + 12 AS NVARCHAR(3))
        + N'}'
FROM #T;

DROP TABLE #T;

DECLARE @rc INT = (SELECT COUNT(*) FROM dbo.products);
DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [2/8] products: %d rows, %d sec', 0, 1, @rc, @el) WITH NOWAIT;
GO

-- ============================================================
-- 3. dbo.orders  (2,000,000 rows)
-- ============================================================
DECLARE @t0 DATETIME2 = GETDATE();
RAISERROR('[3/8] dbo.orders (2M)...', 0, 1) WITH NOWAIT;

SELECT TOP(2000000)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s13,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s14,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s15,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s16
INTO #T
FROM sys.all_objects a, sys.all_objects b
OPTION (MAXDOP 1);

INSERT INTO dbo.orders (
    order_number, customer_id, status, order_date, required_date,
    shipped_date, total_amount, tax_amount, discount_amount, shipping_cost,
    payment_method, payment_status, shipping_addr, tracking_number, notes)
SELECT
    'ORD-' + RIGHT('0000000' + CAST(rn AS VARCHAR(7)), 7),
    -- customer_id: 1 – 500,000
    s1 % 500000 + 1,
    -- status
    CHOOSE(s2 % 5 + 1,
        'PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED'),
    -- order_date: last 3 years
    DATEADD(SECOND, -(s3 % 94608000), GETDATE()),
    -- required_date: order_date + 3-14 days
    CAST(DATEADD(DAY, s4 % 12 + 3,
        DATEADD(SECOND, -(s3 % 94608000), GETDATE())) AS DATE),
    -- shipped_date: 70% non-null
    CASE WHEN s5 % 10 < 7
         THEN DATEADD(DAY, s5 % 6 + 2,
                  DATEADD(SECOND, -(s3 % 94608000), GETDATE()))
         ELSE NULL END,
    -- total_amount: $10 – $5,000
    CAST(s6 % 499000 + 1000 AS MONEY) / 100.0,
    -- tax_amount: 5–12% of $10–$600
    CAST(s7 % 6000 AS MONEY) / 100.0,
    -- discount_amount: 0 – $200
    CAST(s8 % 20001 AS MONEY) / 100.0,
    -- shipping_cost: $0 – $99.99
    CAST(s9 % 9999 AS SMALLMONEY) / 100.0,
    -- payment_method
    CHOOSE(s10 % 6 + 1,
        'CREDIT_CARD','DEBIT_CARD','PAYPAL','BANK_TRANSFER','CRYPTO','CHECK'),
    -- payment_status
    CHOOSE(s11 % 4 + 1,
        'PENDING','PAID','FAILED','REFUNDED'),
    -- shipping_addr
    CAST(s12 % 999 + 1 AS NVARCHAR(4)) + N' '
        + CHOOSE(s13 % 8 + 1,
            N'Main St',N'Oak Ave',N'Maple Dr',N'Cedar Ln',N'Park Blvd',
            N'Washington Ave',N'Lake Rd',N'Hill St')
        + N', '
        + CHOOSE(s14 % 10 + 1,
            N'New York, NY',N'Los Angeles, CA',N'Chicago, IL',N'Houston, TX',N'Phoenix, AZ',
            N'Philadelphia, PA',N'San Antonio, TX',N'San Diego, CA',N'Dallas, TX',N'Austin, TX')
        + N' ' + RIGHT('00000' + CAST(s14 % 99999 AS NVARCHAR(5)), 5),
    -- tracking_number: 70% non-null
    CASE WHEN s15 % 10 < 7
         THEN '1Z' + RIGHT('00000000000000' + CAST(s15 AS VARCHAR(10)), 14)
         ELSE NULL END,
    -- notes: 10% non-null
    CASE WHEN s16 % 10 = 0
         THEN N'Special handling required. Ref: ' + CAST(NEWID() AS NVARCHAR(36))
         ELSE NULL END
FROM #T;

DROP TABLE #T;

DECLARE @rc INT = (SELECT COUNT(*) FROM dbo.orders);
DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [3/8] orders: %d rows, %d sec', 0, 1, @rc, @el) WITH NOWAIT;
GO

-- ============================================================
-- 4. hr.employees  (50,000 rows)
-- ============================================================
DECLARE @t0 DATETIME2 = GETDATE();
RAISERROR('[4/8] hr.employees (50K)...', 0, 1) WITH NOWAIT;

SELECT TOP(50000)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s13,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s14,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s15,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s16
INTO #T
FROM sys.all_objects a, sys.all_objects b
OPTION (MAXDOP 1);

INSERT INTO hr.employees (
    emp_code, first_name, last_name, middle_name, email,
    phone_work, phone_mobile, department, job_title, job_level,
    manager_id, hire_date, termination_dt, salary, bonus_pct,
    is_full_time, office_location, country_code, timezone, bio)
SELECT
    'EMP-' + RIGHT('00000' + CAST(rn AS VARCHAR(5)), 5),
    -- first_name
    CHOOSE(s1 % 20 + 1,
        N'James',N'Mary',N'John',N'Patricia',N'Robert',
        N'Jennifer',N'Michael',N'Linda',N'William',N'Barbara',
        N'David',N'Elizabeth',N'Richard',N'Susan',N'Joseph',
        N'Jessica',N'Thomas',N'Sarah',N'Charles',N'Karen'),
    -- last_name
    CHOOSE(s2 % 20 + 1,
        N'Smith',N'Johnson',N'Williams',N'Brown',N'Jones',
        N'Garcia',N'Miller',N'Davis',N'Rodriguez',N'Martinez',
        N'Hernandez',N'Wilson',N'Anderson',N'Taylor',N'Moore',
        N'Jackson',N'Martin',N'Thompson',N'White',N'Harris'),
    -- middle_name: 40% non-null
    CASE WHEN s3 % 5 < 2
         THEN CHOOSE(s4 % 10 + 1,
             N'Lee',N'Ann',N'Marie',N'Ray',N'Lynn',
             N'Paul',N'Grace',N'Jay',N'Mae',N'Earl')
         ELSE NULL END,
    -- email: unique
    SUBSTRING('abcdefghijklmnopqrstuvwxyz', (rn - 1) % 26 + 1, 1)
        + RIGHT('00000' + CAST(rn AS VARCHAR(5)), 5)
        + '@corp.streamxfer.example.com',
    -- phone_work
    '+1' + RIGHT('0000000000' + CAST(s5 % 800000000 + 200000000 AS VARCHAR(10)), 10),
    -- phone_mobile (80% non-null)
    CASE WHEN s6 % 5 < 4
         THEN '+1' + RIGHT('0000000000' + CAST(s6 % 800000000 + 200000000 AS VARCHAR(10)), 10)
         ELSE NULL END,
    -- department
    CHOOSE(s7 % 10 + 1,
        N'Engineering',N'Sales',N'Marketing',N'Finance',N'HR',
        N'Operations',N'Legal',N'Product',N'Design',N'Support'),
    -- job_title
    CHOOSE(s8 % 10 + 1,
        N'Software Engineer',N'Product Manager',N'Sales Executive',N'Account Manager',N'Data Analyst',
        N'DevOps Engineer',N'UX Designer',N'Financial Analyst',N'HR Specialist',N'Operations Manager'),
    -- job_level: 1–8
    CAST(s9 % 8 + 1 AS TINYINT),
    -- manager_id: top 100 have no manager
    CASE WHEN rn <= 100 THEN NULL
         ELSE s10 % 4900 + 101 END,
    -- hire_date: last 20 years
    CAST(DATEADD(DAY, -(s11 % 7305), GETDATE()) AS DATE),
    -- termination_dt: 5% non-null
    CASE WHEN s12 % 20 = 0
         THEN CAST(DATEADD(DAY, -(s12 % 1825), GETDATE()) AS DATE)
         ELSE NULL END,
    -- salary: $30,000 – $250,000
    CAST(s13 % 220001 + 30000 AS MONEY),
    -- bonus_pct: 0.00 – 30.00
    CAST((s14 % 3001) / 100.0 AS DECIMAL(5,2)),
    -- is_full_time (90% = 1)
    CAST(CASE WHEN s15 % 10 < 9 THEN 1 ELSE 0 END AS BIT),
    -- office_location
    CHOOSE(s16 % 8 + 1,
        N'New York HQ',N'San Francisco',N'Austin TX',N'Chicago',
        N'Seattle',N'Boston',N'Atlanta',N'Remote'),
    'US',
    -- timezone
    CHOOSE(s16 % 6 + 1,
        'America/New_York','America/Chicago','America/Denver',
        'America/Los_Angeles','America/Phoenix','UTC'),
    -- bio: 25% non-null
    CASE WHEN s1 % 4 = 0
         THEN N'Experienced professional with '
             + CAST(s2 % 20 + 1 AS NVARCHAR(3))
             + N' years in the industry.'
         ELSE NULL END
FROM #T;

DROP TABLE #T;

DECLARE @rc INT = (SELECT COUNT(*) FROM hr.employees);
DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [4/8] employees: %d rows, %d sec', 0, 1, @rc, @el) WITH NOWAIT;
GO

-- ============================================================
-- 5. sales.transactions  (2,000,000 rows)
-- ============================================================
DECLARE @t0 DATETIME2 = GETDATE();
RAISERROR('[5/8] sales.transactions (2M)...', 0, 1) WITH NOWAIT;

SELECT TOP(2000000)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s13,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s14,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s15,
    CHECKSUM(NEWID()) & 0x7FFFFFFF AS s16
INTO #T
FROM sys.all_objects a, sys.all_objects b
OPTION (MAXDOP 1);

INSERT INTO sales.transactions (
    account_number, trans_type, amount, currency, exchange_rate, amount_usd,
    trans_datetime, value_date, reference_num, merchant_name, merchant_cat,
    description, status, is_reversed, batch_id, metadata)
SELECT
    -- account_number: 16-digit string
    RIGHT('0000' + CAST(s1 % 10000 AS VARCHAR(4)), 4)
        + RIGHT('0000' + CAST(s2 % 10000 AS VARCHAR(4)), 4)
        + RIGHT('0000' + CAST(s3 % 10000 AS VARCHAR(4)), 4)
        + RIGHT('0000' + CAST(s4 % 10000 AS VARCHAR(4)), 4),
    -- trans_type: CHAR(6) padded
    CHOOSE(s5 % 5 + 1,
        'DEBIT ','CREDIT','XFER  ','FEE   ','REFUND'),
    -- amount: $0.01 – $9,999.99
    CAST(s6 % 999999 + 1 AS DECIMAL(18,4)) / 100.0,
    -- currency
    CHOOSE(s7 % 5 + 1,
        'USD','EUR','GBP','JPY','CAD'),
    -- exchange_rate
    CAST((s8 % 200001 + 800000) / 1000000.0 AS DECIMAL(10,6)),
    -- amount_usd
    CAST(s9 % 999999 + 1 AS DECIMAL(18,4)) / 100.0,
    -- trans_datetime: last 2 years
    DATEADD(SECOND, -(s10 % 63072000), GETDATE()),
    -- value_date
    CAST(DATEADD(DAY, -(s11 % 730), GETDATE()) AS DATE),
    -- reference_num: 30% non-null
    CASE WHEN s12 % 10 < 3
         THEN 'REF' + RIGHT('000000000000' + CAST(s12 AS VARCHAR(12)), 12)
         ELSE NULL END,
    -- merchant_name
    CHOOSE(s13 % 15 + 1,
        N'Amazon',N'Walmart',N'Target',N'Best Buy',N'Starbucks',
        N'McDonald''s',N'Netflix',N'Spotify',N'Apple Store',N'Google Play',
        N'Uber',N'Lyft',N'DoorDash',N'Instacart',N'Airbnb'),
    -- merchant_cat: ISO MCC codes
    CHOOSE(s14 % 10 + 1,
        '5411','5812','5912','7011','5661',
        '4111','5311','5999','7832','5621'),
    -- description
    CHOOSE(s15 % 6 + 1,
        N'Online purchase',N'In-store payment',N'Subscription renewal',
        N'Wire transfer',N'ATM withdrawal',N'Refund processing'),
    -- status
    CHOOSE(s16 % 4 + 1,
        'POSTED','PENDING','CLEARED','REVERSED'),
    -- is_reversed (5% = 1)
    CAST(CASE WHEN s1 % 20 = 0 THEN 1 ELSE 0 END AS BIT),
    -- batch_id
    'BATCH-'
        + CONVERT(VARCHAR(8), DATEADD(DAY, -(s2 % 365), GETDATE()), 112)
        + '-' + RIGHT('0000' + CAST(s3 % 9999 AS VARCHAR(4)), 4),
    -- metadata JSON
    N'{"channel":"'
        + CHOOSE(s4 % 4 + 1, N'web',N'mobile',N'branch',N'api')
        + N'","device_id":"' + CAST(NEWID() AS NVARCHAR(36)) + N'"}'
FROM #T;

DROP TABLE #T;

DECLARE @rc INT = (SELECT COUNT(*) FROM sales.transactions);
DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [5/8] transactions: %d rows, %d sec', 0, 1, @rc, @el) WITH NOWAIT;
GO

-- ============================================================
-- 6. dbo.order_items  (10,000,000 rows — 5 batches × 2M)
-- ============================================================
DECLARE @t0  DATETIME2 = GETDATE();
DECLARE @done INT       = 0;
DECLARE @batch INT      = 2000000;
DECLARE @total INT      = 10000000;
RAISERROR('[6/8] dbo.order_items (10M, 5 x 2M batches)...', 0, 1) WITH NOWAIT;

WHILE @done < @total
BEGIN
    SELECT TOP(@batch)
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7
    INTO #OI
    FROM sys.all_objects a, sys.all_objects b
    OPTION (MAXDOP 1);

    INSERT INTO dbo.order_items (
        order_id, product_id, quantity, unit_price, discount_pct, line_total, warehouse_code)
    SELECT
        s1 % 2000000 + 1,
        s2 % 100000 + 1,
        CAST(s3 % 20 + 1 AS SMALLINT),
        CAST(s4 % 99900 + 100 AS MONEY) / 100.0,
        CAST((s5 % 3001) / 100.0 AS DECIMAL(5,2)),
        CAST(s6 % 999900 + 100 AS MONEY) / 100.0,
        CHOOSE(s7 % 10 + 1,
            'WH01','WH02','WH03','WH04','WH05',
            'WH06','WH07','WH08','WH09','WH10')
    FROM #OI;

    DROP TABLE #OI;
    SET @done = @done + @batch;
    RAISERROR('  order_items: %d / %d rows', 0, 1, @done, @total) WITH NOWAIT;
END

DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [6/8] order_items done: %d sec', 0, 1, @el) WITH NOWAIT;
GO

-- ============================================================
-- 7. dbo.events  (10,000,000 rows — 5 batches × 2M)
-- ============================================================
DECLARE @t0  DATETIME2 = GETDATE();
DECLARE @done INT       = 0;
DECLARE @batch INT      = 2000000;
DECLARE @total INT      = 10000000;
RAISERROR('[7/8] dbo.events (10M, 5 x 2M batches)...', 0, 1) WITH NOWAIT;

WHILE @done < @total
BEGIN
    SELECT TOP(@batch)
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12
    INTO #EV
    FROM sys.all_objects a, sys.all_objects b
    OPTION (MAXDOP 1);

    INSERT INTO dbo.events (
        event_type, source_system, severity, event_time, duration_ms,
        user_id, session_id, ip_address, user_agent, payload,
        error_code, error_message, is_processed, processed_at)
    SELECT
        -- event_type
        CHOOSE(s1 % 10 + 1,
            'USER_LOGIN','USER_LOGOUT','PAGE_VIEW','PURCHASE','API_CALL',
            'ERROR','WARNING','AUDIT','ALERT','DATA_EXPORT'),
        -- source_system
        CHOOSE(s2 % 8 + 1,
            'web-app','mobile-ios','mobile-android','api-gateway',
            'admin-portal','batch-job','webhook','scheduler'),
        -- severity: 0=DEBUG 1=INFO 2=WARN 3=ERROR 4=FATAL
        CAST(s3 % 5 AS TINYINT),
        -- event_time: last 1 year
        DATEADD(SECOND, -(s4 % 31536000), GETDATE()),
        -- duration_ms: 1 – 30,000ms (80% non-null)
        CASE WHEN s5 % 10 < 8 THEN s5 % 29999 + 1 ELSE NULL END,
        -- user_id: 1 – 500,000 (90% non-null)
        CASE WHEN s6 % 10 < 9 THEN s6 % 500000 + 1 ELSE NULL END,
        -- session_id: UUID string
        CAST(NEWID() AS VARCHAR(36)),
        -- ip_address: IPv4
        CAST(s7 % 254 + 1 AS VARCHAR(3)) + '.'
            + CAST(s8 % 256 AS VARCHAR(3)) + '.'
            + CAST(s9 % 256 AS VARCHAR(3)) + '.'
            + CAST(s10 % 254 + 1 AS VARCHAR(3)),
        -- user_agent (60% non-null)
        CASE WHEN s11 % 5 < 3
             THEN CHOOSE(s11 % 5 + 1,
                 N'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
                 N'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36',
                 N'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148',
                 N'Mozilla/5.0 (Linux; Android 14; Pixel 8) Chrome/120.0.0.0 Mobile Safari/537.36',
                 N'python-requests/2.31.0')
             ELSE NULL END,
        -- payload JSON
        N'{"id":"' + CAST(NEWID() AS NVARCHAR(36))
            + N'","ts":' + CAST(DATEDIFF(SECOND, '2020-01-01', GETDATE()) AS NVARCHAR(12))
            + N',"ok":' + CASE WHEN s12 % 10 < 9 THEN N'true' ELSE N'false' END
            + N'}',
        -- error_code: 20% non-null
        CASE WHEN s1 % 5 = 0
             THEN 'ERR-' + CAST(s2 % 9000 + 1000 AS VARCHAR(4))
             ELSE NULL END,
        -- error_message: 20% non-null
        CASE WHEN s3 % 5 = 0
             THEN CHOOSE(s4 % 5 + 1,
                 N'Connection timeout',N'Authentication failed',
                 N'Resource not found',N'Rate limit exceeded',N'Internal server error')
             ELSE NULL END,
        -- is_processed (80% = 1)
        CAST(CASE WHEN s5 % 10 < 8 THEN 1 ELSE 0 END AS BIT),
        -- processed_at: 80% non-null
        CASE WHEN s6 % 10 < 8
             THEN DATEADD(SECOND, s7 % 3600,
                      DATEADD(SECOND, -(s4 % 31536000), GETDATE()))
             ELSE NULL END
    FROM #EV;

    DROP TABLE #EV;
    SET @done = @done + @batch;
    RAISERROR('  events: %d / %d rows', 0, 1, @done, @total) WITH NOWAIT;
END

DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [7/8] events done: %d sec', 0, 1, @el) WITH NOWAIT;
GO

-- ============================================================
-- 8. dbo.measurements  (5,000,000 rows — 3 × 2M + 1M)
-- ============================================================
DECLARE @t0  DATETIME2 = GETDATE();
DECLARE @done INT       = 0;
DECLARE @batch INT      = 2000000;
DECLARE @total INT      = 5000000;
RAISERROR('[8/8] dbo.measurements (5M, batches of 2M)...', 0, 1) WITH NOWAIT;

WHILE @done < @total
BEGIN
    DECLARE @cur_batch INT = CASE WHEN @total - @done < @batch THEN @total - @done ELSE @batch END;

    SELECT TOP(@cur_batch)
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s1,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s2,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s3,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s4,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s5,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s6,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s7,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s8,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s9,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s10,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s11,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s12,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s13,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s14,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s15,
        CHECKSUM(NEWID()) & 0x7FFFFFFF AS s16
    INTO #MS
    FROM sys.all_objects a, sys.all_objects b
    OPTION (MAXDOP 1);

    INSERT INTO dbo.measurements (
        sensor_id, device_type, measured_at, temperature_c, pressure_hpa,
        humidity_pct, voltage_v, current_a, power_watts,
        latitude, longitude, altitude_m,
        signal_strength, battery_pct, raw_bytes, checksum)
    SELECT
        -- sensor_id: DEV-XXXXX-NN
        'DEV-'
            + RIGHT('00000' + CAST(s1 % 99999 + 1 AS VARCHAR(5)), 5)
            + '-' + RIGHT('00' + CAST(s2 % 99 AS VARCHAR(2)), 2),
        -- device_type
        CHOOSE(s3 % 8 + 1,
            'temperature-sensor','pressure-sensor','flow-meter','current-sensor',
            'vibration-sensor','humidity-sensor','power-meter','environmental'),
        -- measured_at: last 6 months
        DATEADD(SECOND, -(s4 % 15552000), GETDATE()),
        -- temperature_c: –40 – +85 °C
        CAST((s5 % 12501 - 4000) AS REAL) / 100.0,
        -- pressure_hpa: 800 – 1200 hPa
        CAST(s6 % 40001 + 80000 AS FLOAT) / 100.0,
        -- humidity_pct: 0.00 – 100.00
        CAST((s7 % 10001) / 100.0 AS DECIMAL(5,2)),
        -- voltage_v: 0 – 48 V
        CAST(s8 % 4801 AS REAL) / 100.0,
        -- current_a: 0 – 20 A
        CAST(s9 % 2001 AS REAL) / 100.0,
        -- power_watts: 0 – 1000 W
        CAST(s10 % 100001 AS FLOAT) / 100.0,
        -- latitude: –90 – +90
        CAST(s11 % 18000000 AS BIGINT) / 100000.0 - 90.0,
        -- longitude: –180 – +180
        CAST(s12 % 36000000 AS BIGINT) / 100000.0 - 180.0,
        -- altitude_m: –100 – +8800 m
        CAST((s13 % 889001 - 10000) AS REAL) / 100.0,
        -- signal_strength: –120 – 0 dBm
        CAST(-(s14 % 121) AS SMALLINT),
        -- battery_pct: 0 – 100
        CAST(s15 % 101 AS TINYINT),
        -- raw_bytes: 32 bytes of random binary
        CAST(NEWID() AS BINARY(16)) + CAST(NEWID() AS BINARY(16)),
        -- checksum: 16-byte
        CAST(NEWID() AS BINARY(16))
    FROM #MS;

    DROP TABLE #MS;
    SET @done = @done + @cur_batch;
    RAISERROR('  measurements: %d / %d rows', 0, 1, @done, @total) WITH NOWAIT;
END

DECLARE @el INT = DATEDIFF(SECOND, @t0, GETDATE());
RAISERROR('  [8/8] measurements done: %d sec', 0, 1, @el) WITH NOWAIT;
GO

-- ============================================================
-- Summary
-- ============================================================
RAISERROR('==============================================', 0, 1) WITH NOWAIT;
RAISERROR('Seeding complete — final row counts:', 0, 1) WITH NOWAIT;
SELECT 'dbo.customers'        AS tbl, COUNT(*) AS rows FROM dbo.customers UNION ALL
SELECT 'dbo.products',                COUNT(*)         FROM dbo.products  UNION ALL
SELECT 'dbo.orders',                  COUNT(*)         FROM dbo.orders    UNION ALL
SELECT 'dbo.order_items',             COUNT(*)         FROM dbo.order_items UNION ALL
SELECT 'dbo.events',                  COUNT(*)         FROM dbo.events    UNION ALL
SELECT 'dbo.measurements',            COUNT(*)         FROM dbo.measurements UNION ALL
SELECT 'sales.transactions',          COUNT(*)         FROM sales.transactions UNION ALL
SELECT 'hr.employees',                COUNT(*)         FROM hr.employees
ORDER BY tbl;
GO
