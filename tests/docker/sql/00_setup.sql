-- ============================================================
-- StreamXfer Test Database — Schema Setup
-- Creates: database, schemas (sales / hr), 8 tables
-- Safe to re-run: IF NOT EXISTS guards on every object
-- ============================================================

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'streamxfer_test')
    CREATE DATABASE streamxfer_test;
GO

USE streamxfer_test;
GO

-- ---- schemas -----------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sales')
    EXEC('CREATE SCHEMA sales');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'hr')
    EXEC('CREATE SCHEMA hr');
GO

-- ============================================================
-- dbo.customers
-- Wide row: strings, dates, decimals, money, bit, nvarchar(max)
-- ============================================================
IF OBJECT_ID('dbo.customers', 'U') IS NULL
CREATE TABLE dbo.customers (
    customer_id     INT                 IDENTITY(1,1)   PRIMARY KEY,
    guid_id         UNIQUEIDENTIFIER    DEFAULT NEWID() NOT NULL,
    first_name      NVARCHAR(100)       NOT NULL,
    last_name       NVARCHAR(100)       NOT NULL,
    email           VARCHAR(255),
    phone           VARCHAR(20),
    birth_date      DATE,
    gender          CHAR(1),
    address_line1   NVARCHAR(200),
    address_line2   NVARCHAR(200),
    city            NVARCHAR(100),
    state_province  NVARCHAR(50),
    country_code    CHAR(2)             NOT NULL DEFAULT 'US',
    postal_code     VARCHAR(20),
    is_active       BIT                 NOT NULL DEFAULT 1,
    credit_limit    DECIMAL(18,2),
    loyalty_points  BIGINT              NOT NULL DEFAULT 0,
    account_balance MONEY,
    registration_dt DATETIME2(7)        NOT NULL,
    last_login      DATETIME2(3),
    notes           NVARCHAR(MAX),
    created_at      DATETIME2           NOT NULL DEFAULT GETDATE(),
    updated_at      DATETIME2           NOT NULL DEFAULT GETDATE()
);
GO

-- ============================================================
-- dbo.products
-- money, float, decimal, bit, nvarchar(max) JSON specs
-- ============================================================
IF OBJECT_ID('dbo.products', 'U') IS NULL
CREATE TABLE dbo.products (
    product_id      INT                 IDENTITY(1,1)   PRIMARY KEY,
    sku             VARCHAR(50)         NOT NULL UNIQUE,
    product_name    NVARCHAR(200)       NOT NULL,
    description     NVARCHAR(MAX),
    category        NVARCHAR(100)       NOT NULL,
    sub_category    NVARCHAR(100),
    brand           NVARCHAR(100),
    unit_price      MONEY               NOT NULL,
    cost_price      SMALLMONEY          NOT NULL,
    weight_kg       REAL,
    stock_qty       INT                 NOT NULL DEFAULT 0,
    reorder_level   INT                 NOT NULL DEFAULT 10,
    is_available    BIT                 NOT NULL DEFAULT 1,
    rating          DECIMAL(3,2),
    review_count    INT                 NOT NULL DEFAULT 0,
    image_url       VARCHAR(500),
    tags            NVARCHAR(500),
    specifications  NVARCHAR(MAX),
    created_at      DATETIME2           NOT NULL DEFAULT GETDATE()
);
GO

-- ============================================================
-- dbo.orders
-- datetime2, date, money, smallmoney, nvarchar(max)
-- ============================================================
IF OBJECT_ID('dbo.orders', 'U') IS NULL
CREATE TABLE dbo.orders (
    order_id        INT                 IDENTITY(1,1)   PRIMARY KEY,
    order_number    VARCHAR(30)         NOT NULL UNIQUE,
    customer_id     INT                 NOT NULL,
    status          VARCHAR(20)         NOT NULL DEFAULT 'PENDING',
    order_date      DATETIME2(3)        NOT NULL,
    required_date   DATE,
    shipped_date    DATETIME2(3),
    total_amount    MONEY               NOT NULL,
    tax_amount      MONEY               NOT NULL DEFAULT 0,
    discount_amount MONEY               NOT NULL DEFAULT 0,
    shipping_cost   SMALLMONEY          NOT NULL DEFAULT 0,
    payment_method  VARCHAR(20),
    payment_status  VARCHAR(20)         NOT NULL DEFAULT 'PENDING',
    shipping_addr   NVARCHAR(500),
    tracking_number VARCHAR(50),
    notes           NVARCHAR(MAX),
    created_at      DATETIME2           NOT NULL DEFAULT GETDATE()
);
GO

-- ============================================================
-- dbo.order_items
-- smallint, decimal, char, money
-- ============================================================
IF OBJECT_ID('dbo.order_items', 'U') IS NULL
CREATE TABLE dbo.order_items (
    item_id         BIGINT              IDENTITY(1,1)   PRIMARY KEY,
    order_id        INT                 NOT NULL,
    product_id      INT                 NOT NULL,
    quantity        SMALLINT            NOT NULL DEFAULT 1,
    unit_price      MONEY               NOT NULL,
    discount_pct    DECIMAL(5,2)        NOT NULL DEFAULT 0.00,
    line_total      MONEY               NOT NULL,
    warehouse_code  CHAR(4),
    notes           NVARCHAR(200)
);
GO

-- ============================================================
-- dbo.events
-- tinyint severity, bigint, bit, nvarchar(max), uniqueidentifier
-- ============================================================
IF OBJECT_ID('dbo.events', 'U') IS NULL
CREATE TABLE dbo.events (
    event_id        BIGINT              IDENTITY(1,1)   PRIMARY KEY,
    event_uuid      UNIQUEIDENTIFIER    DEFAULT NEWID() NOT NULL,
    event_type      VARCHAR(50)         NOT NULL,
    source_system   VARCHAR(50)         NOT NULL,
    severity        TINYINT             NOT NULL DEFAULT 0,
    event_time      DATETIME2(7)        NOT NULL,
    duration_ms     INT,
    user_id         INT,
    session_id      VARCHAR(64),
    ip_address      VARCHAR(45),
    user_agent      NVARCHAR(500),
    payload         NVARCHAR(MAX),
    error_code      VARCHAR(20),
    error_message   NVARCHAR(500),
    is_processed    BIT                 NOT NULL DEFAULT 0,
    processed_at    DATETIME2
);
GO

-- ============================================================
-- dbo.measurements
-- real, float, decimal, varbinary(256), binary(16)
-- ============================================================
IF OBJECT_ID('dbo.measurements', 'U') IS NULL
CREATE TABLE dbo.measurements (
    meas_id         BIGINT              IDENTITY(1,1)   PRIMARY KEY,
    sensor_id       VARCHAR(50)         NOT NULL,
    device_type     VARCHAR(30)         NOT NULL,
    measured_at     DATETIME2(7)        NOT NULL,
    temperature_c   REAL,
    pressure_hpa    FLOAT,
    humidity_pct    DECIMAL(5,2),
    voltage_v       REAL,
    current_a       REAL,
    power_watts     FLOAT,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    altitude_m      REAL,
    signal_strength SMALLINT,
    battery_pct     TINYINT,
    raw_bytes       VARBINARY(256),
    checksum        BINARY(16)
);
GO

-- ============================================================
-- sales.transactions
-- datetimeoffset-style dt2, decimal(18,4), char(3), nvarchar(max)
-- ============================================================
IF OBJECT_ID('sales.transactions', 'U') IS NULL
CREATE TABLE sales.transactions (
    trans_id        BIGINT              IDENTITY(1,1)   PRIMARY KEY,
    trans_uuid      UNIQUEIDENTIFIER    DEFAULT NEWID() NOT NULL,
    account_number  VARCHAR(16)         NOT NULL,
    trans_type      CHAR(6)             NOT NULL,
    amount          DECIMAL(18,4)       NOT NULL,
    currency        CHAR(3)             NOT NULL DEFAULT 'USD',
    exchange_rate   DECIMAL(10,6)       NOT NULL DEFAULT 1.000000,
    amount_usd      DECIMAL(18,4)       NOT NULL,
    trans_datetime  DATETIME2(7)        NOT NULL,
    value_date      DATE                NOT NULL,
    reference_num   VARCHAR(30),
    merchant_name   NVARCHAR(200),
    merchant_cat    VARCHAR(10),
    description     NVARCHAR(500),
    status          VARCHAR(10)         NOT NULL DEFAULT 'POSTED',
    is_reversed     BIT                 NOT NULL DEFAULT 0,
    batch_id        VARCHAR(30),
    metadata        NVARCHAR(MAX)
);
GO

-- ============================================================
-- hr.employees
-- self-ref manager_id, date, money, nvarchar(max)
-- ============================================================
IF OBJECT_ID('hr.employees', 'U') IS NULL
CREATE TABLE hr.employees (
    emp_id          INT                 IDENTITY(1,1)   PRIMARY KEY,
    emp_code        VARCHAR(20)         NOT NULL UNIQUE,
    first_name      NVARCHAR(100)       NOT NULL,
    last_name       NVARCHAR(100)       NOT NULL,
    middle_name     NVARCHAR(100),
    email           VARCHAR(200)        NOT NULL UNIQUE,
    phone_work      VARCHAR(20),
    phone_mobile    VARCHAR(20),
    department      NVARCHAR(100)       NOT NULL,
    job_title       NVARCHAR(200)       NOT NULL,
    job_level       TINYINT             NOT NULL DEFAULT 1,
    manager_id      INT,
    hire_date       DATE                NOT NULL,
    termination_dt  DATE,
    salary          MONEY               NOT NULL,
    bonus_pct       DECIMAL(5,2)        NOT NULL DEFAULT 0.00,
    is_full_time    BIT                 NOT NULL DEFAULT 1,
    office_location NVARCHAR(100),
    country_code    CHAR(2)             NOT NULL DEFAULT 'US',
    timezone        VARCHAR(50),
    bio             NVARCHAR(MAX),
    created_at      DATETIME2           NOT NULL DEFAULT GETDATE()
);
GO

RAISERROR('Schema setup complete.', 0, 1) WITH NOWAIT;
GO
