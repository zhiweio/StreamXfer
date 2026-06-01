//! StreamXfer Test Data Seeder
//!
//! Generates rich, realistic test data in SQL Server for local development/testing.
//!
//! Tables created (8 total, 3 schemas, ~14 300 rows):
//!   dbo.customers (1 000)   – wide row: strings, dates, decimals, money, bit, nvarchar(max)
//!   dbo.products  (500)     – money, float, decimal, bit, nvarchar(max) JSON specs
//!   dbo.orders    (2 000)   – datetime2, date, money, smallmoney
//!   dbo.order_items(5 000)  – smallint, decimal, char
//!   dbo.events    (3 000)   – tinyint severity, varbinary-free, bigint, bit
//!   dbo.measurements(2 000) – real, float, decimal, varbinary(256), binary(16)
//!   sales.transactions(1 500) – datetimeoffset-style dt2, decimal(18,4), char(3)
//!   hr.employees  (300)     – self-ref manager_id, date, money, nvarchar(max)
//!
//! Usage:
//!   SQL_SERVER_HOST=localhost SA_PASSWORD="StreamXfer@2024!" cargo run
//!
//! Defaults: host=localhost, port=1433, user=sa, password=StreamXfer@2024!

use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use fake::faker::address::en::*;
use fake::faker::company::en::*;
use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::faker::phone_number::en::*;
use fake::{Fake};
use rand::Rng;
use tiberius::{AuthMethod, Client, Config, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use uuid::Uuid;

type MssqlClient = Client<tokio_util::compat::Compat<TcpStream>>;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Pick a random element from a `&[&str]` slice.
fn pick<'a>(items: &[&'a str], rng: &mut impl Rng) -> &'a str {
    items[rng.gen_range(0..items.len())]
}

/// Random `NaiveDateTime` up to `days` days in the past.
fn past_dt(days: i64) -> NaiveDateTime {
    let secs = rand::thread_rng().gen_range(0..days * 86_400);
    (Utc::now() - Duration::seconds(secs)).naive_utc()
}

fn past_date(days: i64) -> NaiveDate {
    past_dt(days).date()
}

/// Convert a `NaiveDate` to midnight `NaiveDateTime` for tiberius binding.
/// SQL Server implicitly converts DATETIME2 → DATE for DATE columns.
fn as_dt(d: NaiveDate) -> NaiveDateTime {
    d.and_hms_opt(0, 0, 0).unwrap()
}

fn opt_as_dt(d: Option<NaiveDate>) -> Option<NaiveDateTime> {
    d.map(as_dt)
}

// ─── connection ──────────────────────────────────────────────────────────────

async fn connect(database: Option<&str>) -> Result<MssqlClient> {
    let host = env("SQL_SERVER_HOST", "localhost");
    let pass = env("SA_PASSWORD", "StreamXfer@2024!");

    let mut cfg = Config::new();
    cfg.host(&host);
    cfg.port(1433);
    cfg.authentication(AuthMethod::sql_server("sa", pass.as_str()));
    cfg.trust_cert(); // dev-only: skip TLS cert validation

    if let Some(db) = database {
        cfg.database(db);
    }

    let tcp = TcpStream::connect(cfg.get_addr())
        .await
        .context("Cannot reach SQL Server – is it running?")?;
    tcp.set_nodelay(true)?;

    Client::connect(cfg, tcp.compat_write())
        .await
        .context("TDS handshake failed")
}

// ─── DDL ─────────────────────────────────────────────────────────────────────

async fn create_database(client: &mut MssqlClient) -> Result<()> {
    client
        .execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'streamxfer_test') \
             CREATE DATABASE streamxfer_test",
            &[],
        )
        .await?;
    Ok(())
}

async fn create_schemas(db: &mut MssqlClient) -> Result<()> {
    for schema in &["sales", "hr"] {
        db.execute(
            &format!(
                "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}') \
                 EXEC('CREATE SCHEMA {schema}')"
            ),
            &[],
        )
        .await?;
    }
    Ok(())
}

async fn create_tables(db: &mut MssqlClient) -> Result<()> {
    // dbo.customers – wide row with most SQL Server types
    db.execute(
        "IF OBJECT_ID('dbo.customers','U') IS NULL
         CREATE TABLE dbo.customers (
             customer_id     INT           IDENTITY(1,1) PRIMARY KEY,
             guid_id         UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL,
             first_name      NVARCHAR(100) NOT NULL,
             last_name       NVARCHAR(100) NOT NULL,
             email           VARCHAR(255),
             phone           VARCHAR(20),
             birth_date      DATE,
             gender          CHAR(1),
             address_line1   NVARCHAR(200),
             address_line2   NVARCHAR(200),
             city            NVARCHAR(100),
             state_province  NVARCHAR(50),
             country_code    CHAR(2)       NOT NULL DEFAULT 'US',
             postal_code     VARCHAR(20),
             is_active       BIT           NOT NULL DEFAULT 1,
             credit_limit    DECIMAL(18,2),
             loyalty_points  BIGINT        NOT NULL DEFAULT 0,
             account_balance MONEY,
             registration_dt DATETIME2(7)  NOT NULL,
             last_login      DATETIME2(3),
             notes           NVARCHAR(MAX),
             created_at      DATETIME2     NOT NULL DEFAULT GETDATE(),
             updated_at      DATETIME2     NOT NULL DEFAULT GETDATE()
         )",
        &[],
    )
    .await?;

    // dbo.products
    db.execute(
        "IF OBJECT_ID('dbo.products','U') IS NULL
         CREATE TABLE dbo.products (
             product_id    INT           IDENTITY(1,1) PRIMARY KEY,
             sku           VARCHAR(50)   NOT NULL UNIQUE,
             product_name  NVARCHAR(200) NOT NULL,
             description   NVARCHAR(MAX),
             category      NVARCHAR(100),
             sub_category  NVARCHAR(100),
             brand         NVARCHAR(100),
             unit_price    MONEY         NOT NULL,
             cost_price    DECIMAL(10,4),
             weight_kg     FLOAT,
             stock_qty     INT           NOT NULL DEFAULT 0,
             reorder_level SMALLINT      NOT NULL DEFAULT 10,
             is_available  BIT           NOT NULL DEFAULT 1,
             rating        DECIMAL(3,2),
             review_count  INT           NOT NULL DEFAULT 0,
             image_url     VARCHAR(500),
             tags          NVARCHAR(500),
             specifications NVARCHAR(MAX),
             created_at    DATETIME2     NOT NULL DEFAULT GETDATE(),
             updated_at    DATETIME2     NOT NULL DEFAULT GETDATE()
         )",
        &[],
    )
    .await?;

    // dbo.orders
    db.execute(
        "IF OBJECT_ID('dbo.orders','U') IS NULL
         CREATE TABLE dbo.orders (
             order_id        BIGINT       IDENTITY(1,1) PRIMARY KEY,
             order_number    VARCHAR(30)  NOT NULL UNIQUE,
             customer_id     INT          NOT NULL,
             status          VARCHAR(20)  NOT NULL,
             order_date      DATETIME2    NOT NULL,
             required_date   DATE,
             shipped_date    DATETIME2,
             total_amount    MONEY        NOT NULL,
             tax_amount      MONEY        NOT NULL DEFAULT 0,
             discount_amount MONEY        NOT NULL DEFAULT 0,
             shipping_cost   SMALLMONEY   NOT NULL DEFAULT 0,
             payment_method  VARCHAR(30),
             payment_status  VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
             shipping_addr   NVARCHAR(500),
             tracking_number VARCHAR(50),
             notes           NVARCHAR(1000),
             created_at      DATETIME2    NOT NULL DEFAULT GETDATE()
         )",
        &[],
    )
    .await?;

    // dbo.order_items
    db.execute(
        "IF OBJECT_ID('dbo.order_items','U') IS NULL
         CREATE TABLE dbo.order_items (
             item_id        INT         IDENTITY(1,1) PRIMARY KEY,
             order_id       BIGINT      NOT NULL,
             product_id     INT         NOT NULL,
             quantity       SMALLINT    NOT NULL,
             unit_price     MONEY       NOT NULL,
             discount_pct   DECIMAL(5,2) NOT NULL DEFAULT 0.00,
             line_total     MONEY        NOT NULL,
             warehouse_code CHAR(5),
             notes          VARCHAR(200)
         )",
        &[],
    )
    .await?;

    // dbo.events  (audit / application log)
    db.execute(
        "IF OBJECT_ID('dbo.events','U') IS NULL
         CREATE TABLE dbo.events (
             event_id      BIGINT       IDENTITY(1,1) PRIMARY KEY,
             event_uuid    UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL,
             event_type    VARCHAR(50)  NOT NULL,
             source_system VARCHAR(50),
             severity      TINYINT      NOT NULL DEFAULT 0,
             event_time    DATETIME2(7) NOT NULL,
             duration_ms   INT,
             user_id       INT,
             session_id    VARCHAR(64),
             ip_address    VARCHAR(45),
             user_agent    NVARCHAR(500),
             payload       NVARCHAR(MAX),
             error_code    VARCHAR(20),
             error_message NVARCHAR(500),
             is_processed  BIT          NOT NULL DEFAULT 0,
             processed_at  DATETIME2
         )",
        &[],
    )
    .await?;

    // dbo.measurements  (IoT sensor data)
    db.execute(
        "IF OBJECT_ID('dbo.measurements','U') IS NULL
         CREATE TABLE dbo.measurements (
             measure_id      BIGINT       IDENTITY(1,1) PRIMARY KEY,
             sensor_id       VARCHAR(50)  NOT NULL,
             device_type     VARCHAR(50),
             measured_at     DATETIME2(7) NOT NULL,
             temperature_c   REAL,
             pressure_hpa    FLOAT,
             humidity_pct    DECIMAL(5,2),
             voltage_v       DECIMAL(8,4),
             current_a       DECIMAL(8,4),
             power_watts     FLOAT,
             latitude        DECIMAL(9,6),
             longitude       DECIMAL(9,6),
             altitude_m      FLOAT,
             signal_strength SMALLINT,
             battery_pct     TINYINT,
             raw_bytes       VARBINARY(256),
             checksum        BINARY(16)
         )",
        &[],
    )
    .await?;

    // sales.transactions  (financial ledger)
    db.execute(
        "IF OBJECT_ID('sales.transactions','U') IS NULL
         CREATE TABLE sales.transactions (
             trans_id      BIGINT        IDENTITY(1,1) PRIMARY KEY,
             trans_uuid    UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL,
             account_number VARCHAR(30)  NOT NULL,
             trans_type    VARCHAR(20)   NOT NULL,
             amount        DECIMAL(18,4) NOT NULL,
             currency      CHAR(3)       NOT NULL DEFAULT 'USD',
             exchange_rate FLOAT         NOT NULL DEFAULT 1.0,
             amount_usd    MONEY,
             trans_datetime DATETIME2(7) NOT NULL,
             value_date    DATE,
             reference_num VARCHAR(50),
             merchant_name NVARCHAR(200),
             merchant_cat  VARCHAR(10),
             description   NVARCHAR(500),
             status        VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
             is_reversed   BIT           NOT NULL DEFAULT 0,
             batch_id      VARCHAR(30),
             metadata      NVARCHAR(MAX)
         )",
        &[],
    )
    .await?;

    // hr.employees  (with self-referential manager_id)
    db.execute(
        "IF OBJECT_ID('hr.employees','U') IS NULL
         CREATE TABLE hr.employees (
             employee_id    INT          IDENTITY(1,1) PRIMARY KEY,
             emp_code       VARCHAR(20)  NOT NULL UNIQUE,
             first_name     NVARCHAR(100) NOT NULL,
             last_name      NVARCHAR(100) NOT NULL,
             middle_name    NVARCHAR(100),
             email          VARCHAR(255) NOT NULL UNIQUE,
             phone_work     VARCHAR(20),
             phone_mobile   VARCHAR(20),
             department     NVARCHAR(100),
             job_title      NVARCHAR(150),
             job_level      TINYINT,
             manager_id     INT,
             hire_date      DATE         NOT NULL,
             termination_dt DATE,
             salary         MONEY,
             bonus_pct      DECIMAL(5,2),
             is_full_time   BIT          NOT NULL DEFAULT 1,
             office_location NVARCHAR(200),
             country_code   CHAR(2)      NOT NULL DEFAULT 'US',
             timezone       VARCHAR(50),
             bio            NVARCHAR(MAX),
             created_at     DATETIME2    NOT NULL DEFAULT GETDATE()
         )",
        &[],
    )
    .await?;

    println!("  ✓ 8 tables ready");
    Ok(())
}

// ─── static reference data ────────────────────────────────────────────────────

const ORDER_STATUSES: &[&str] = &[
    "PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED",
];
const PAYMENT_METHODS: &[&str] = &[
    "CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO", "GIFT_CARD",
];
const EVENT_TYPES: &[&str] = &[
    "USER_LOGIN", "USER_LOGOUT", "PAGE_VIEW", "PURCHASE", "API_CALL",
    "ERROR", "WARNING", "AUDIT", "SYSTEM_ALERT", "DATA_EXPORT",
];
const SOURCE_SYSTEMS: &[&str] = &[
    "web-app", "mobile-ios", "mobile-android", "api-gateway", "admin-portal", "batch-job",
];
const DEVICE_TYPES: &[&str] = &[
    "temperature-sensor", "pressure-gauge", "humidity-sensor", "smart-meter", "weather-station",
];
const CURRENCIES: &[&str] = &["USD", "EUR", "GBP", "JPY", "CNY", "CAD", "AUD", "CHF"];
const TRANS_TYPES: &[&str] = &[
    "PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "FEE", "ADJUSTMENT",
];
const MERCHANT_CATS: &[&str] = &["5411", "5812", "7011", "5912", "5999", "4814", "7372", "5310"];
const DEPARTMENTS: &[&str] = &[
    "Engineering", "Sales", "Marketing", "Finance", "HR",
    "Operations", "Product", "Legal", "Customer Success",
];
const WAREHOUSES: &[&str] = &["WH-01", "WH-02", "WH-03", "WH-EU", "WH-AP"];
const CATEGORIES: &[&str] = &[
    "Electronics", "Clothing", "Home & Garden", "Sports", "Toys",
    "Books", "Food & Beverage", "Health & Beauty", "Automotive", "Office Supplies",
];
const TIMEZONES: &[&str] = &[
    "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
    "Europe/London", "Europe/Paris", "Asia/Tokyo", "Asia/Shanghai", "Australia/Sydney",
];

// ─── seeding functions ────────────────────────────────────────────────────────

async fn seed_customers(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  customers       ({count:>5}) ... ");
    let mut rng = rand::thread_rng();

    for _ in 0..count {
        let first: String = FirstName().fake();
        let last: String = LastName().fake();
        let email: Option<String> = if rng.gen_bool(0.95) {
            Some(FreeEmail().fake())
        } else {
            None
        };
        let phone: Option<String> = if rng.gen_bool(0.8) {
            let raw: String = PhoneNumber().fake();
            Some(raw.chars().filter(|c| c.is_ascii_digit() || *c == '+').take(20).collect())
        } else {
            None
        };
        let birth_date = NaiveDate::from_ymd_opt(
            rng.gen_range(1950..=2000),
            rng.gen_range(1..=12),
            rng.gen_range(1..=28),
        )
        .unwrap();
        let gender: Option<&str> = match rng.gen_range(0u8..3) {
            0 => Some("M"),
            1 => Some("F"),
            _ => None,
        };
        let addr1 = format!(
            "{} {}",
            rng.gen_range(1u32..=9999),
            StreetName().fake::<String>()
        );
        let addr2: Option<String> = if rng.gen_bool(0.2) {
            Some(format!("Apt {}", rng.gen_range(1u16..=999)))
        } else {
            None
        };
        let city: String = CityName().fake();
        let state: String = StateAbbr().fake();
        let postal: String = ZipCode().fake();
        let is_active = rng.gen_bool(0.92);
        let credit_limit: f64 = rng.gen_range(500..=50_000) as f64;
        let loyalty_points: i64 = rng.gen_range(0..=200_000);
        let balance: f64 = (rng.gen_range(-500_i32..=50_000) as f64 * 100.0).round() / 100.0;
        let reg_dt = past_dt(1825);
        let last_login: Option<NaiveDateTime> =
            if is_active { Some(past_dt(90)) } else { None };
        let notes: Option<String> =
            if rng.gen_bool(0.3) { Some(Sentence(5..12).fake()) } else { None };

        let mut q = Query::new(
            "INSERT INTO dbo.customers
             (first_name,last_name,email,phone,birth_date,gender,address_line1,address_line2,
              city,state_province,country_code,postal_code,is_active,credit_limit,loyalty_points,
              account_balance,registration_dt,last_login,notes)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15,
                    @P16,@P17,@P18,@P19)",
        );
        q.bind(first.as_str());
        q.bind(last.as_str());
        q.bind(email.as_deref());
        q.bind(phone.as_deref());
        q.bind(as_dt(birth_date));
        q.bind(gender);
        q.bind(addr1.as_str());
        q.bind(addr2.as_deref());
        q.bind(city.as_str());
        q.bind(state.as_str());
        q.bind("US");
        q.bind(postal.as_str());
        q.bind(is_active);
        q.bind(credit_limit);
        q.bind(loyalty_points);
        q.bind(balance);
        q.bind(reg_dt);
        q.bind(last_login);
        q.bind(notes.as_deref());
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_products(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  products        ({count:>5}) ... ");
    let mut rng = rand::thread_rng();
    let adjectives = ["Premium", "Standard", "Deluxe", "Basic", "Pro", "Ultra", "Lite", "Smart"];
    let suffixes = ["Kit", "Set", "Pack", "Bundle", "Edition", "Series", "Collection"];

    for i in 0..count {
        let sku = format!("SKU-{:06}", i + 1);
        let name = format!(
            "{} {} {}",
            pick(&adjectives, &mut rng),
            CompanyName().fake::<String>()
                .split_whitespace()
                .next()
                .unwrap_or("Item"),
            pick(&suffixes, &mut rng)
        );
        let desc: String = Sentences(2..4).fake::<Vec<String>>().join(" ");
        let category = pick(CATEGORIES, &mut rng);
        let brand: String = CompanyName().fake();
        let unit_price: f64 = rng.gen_range(99u32..=99_900) as f64 / 100.0;
        let cost_price: f64 = (unit_price * rng.gen_range(40..=70) as f64 / 100.0 * 10000.0).round() / 10000.0;
        let weight: Option<f64> =
            if rng.gen_bool(0.85) { Some(rng.gen_range(1..=50_000) as f64 / 1000.0) } else { None };
        let stock: i32 = rng.gen_range(0..=5000);
        let reorder: i16 = rng.gen_range(5i16..=100);
        let is_available = stock > 0 && rng.gen_bool(0.9);
        let rating: Option<f64> =
            if rng.gen_bool(0.8) { Some(rng.gen_range(10u32..=50) as f64 / 10.0) } else { None };
        let review_count: i32 =
            if rating.is_some() { rng.gen_range(0..=5000) } else { 0 };
        let words: Vec<String> = Words(2..5).fake();
        let tags = words.join(",");
        let specs = format!(
            r#"{{"dims":"{}x{}x{}cm","material":"{}","warranty_months":{}}}"#,
            rng.gen_range(5u32..100),
            rng.gen_range(5u32..100),
            rng.gen_range(2u32..50),
            pick(&["plastic", "metal", "wood", "fabric", "glass"], &mut rng),
            rng.gen_range(0u32..=36)
        );

        let mut q = Query::new(
            "INSERT INTO dbo.products
             (sku,product_name,description,category,brand,unit_price,cost_price,weight_kg,
              stock_qty,reorder_level,is_available,rating,review_count,tags,specifications)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15)",
        );
        q.bind(sku.as_str());
        q.bind(name.as_str());
        q.bind(desc.as_str());
        q.bind(category);
        q.bind(brand.as_str());
        q.bind(unit_price);
        q.bind(cost_price);
        q.bind(weight);
        q.bind(stock);
        q.bind(reorder);
        q.bind(is_available);
        q.bind(rating);
        q.bind(review_count);
        q.bind(tags.as_str());
        q.bind(specs.as_str());
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_orders(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  orders          ({count:>5}) ... ");
    let mut rng = rand::thread_rng();

    for i in 0..count {
        let order_number = format!("ORD-{}-{:06}", Utc::now().format("%Y"), i + 1);
        let customer_id: i32 = rng.gen_range(1..=1000);
        let status = pick(ORDER_STATUSES, &mut rng);
        let order_date = past_dt(730);
        let required_date: NaiveDate =
            (order_date + Duration::days(rng.gen_range(3..=30))).date();
        let shipped_date: Option<NaiveDateTime> =
            if status == "SHIPPED" || status == "DELIVERED" {
                Some(order_date + Duration::hours(rng.gen_range(24..=120)))
            } else {
                None
            };
        let total: f64 = rng.gen_range(100u32..=100_000) as f64 / 100.0;
        let tax: f64 = (total * 0.08 * 100.0).round() / 100.0;
        let discount: f64 = if rng.gen_bool(0.3) {
            (total * rng.gen_range(5u32..=20) as f64 / 100.0 * 100.0).round() / 100.0
        } else {
            0.0
        };
        let shipping: f64 =
            if total > 100.0 { 0.0 } else { rng.gen_range(499u32..=2999) as f64 / 100.0 };
        let payment_method: Option<&str> =
            if rng.gen_bool(0.95) { Some(pick(PAYMENT_METHODS, &mut rng)) } else { None };
        let payment_status = if status == "PENDING" { "PENDING" } else { "PAID" };
        let shipping_addr = format!(
            "{} {}, {} {} US",
            rng.gen_range(1u32..=9999),
            StreetName().fake::<String>(),
            CityName().fake::<String>(),
            StateAbbr().fake::<String>()
        );
        let tracking: Option<String> = if shipped_date.is_some() {
            Some(format!("1Z{:016X}", rng.gen::<u64>()))
        } else {
            None
        };
        let notes: Option<String> =
            if rng.gen_bool(0.15) { Some(Sentence(3..8).fake()) } else { None };

        let mut q = Query::new(
            "INSERT INTO dbo.orders
             (order_number,customer_id,status,order_date,required_date,shipped_date,
              total_amount,tax_amount,discount_amount,shipping_cost,payment_method,
              payment_status,shipping_addr,tracking_number,notes)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15)",
        );
        q.bind(order_number.as_str());
        q.bind(customer_id);
        q.bind(status);
        q.bind(order_date);
        q.bind(as_dt(required_date));
        q.bind(shipped_date);
        q.bind(total);
        q.bind(tax);
        q.bind(discount);
        q.bind(shipping);
        q.bind(payment_method);
        q.bind(payment_status);
        q.bind(shipping_addr.as_str());
        q.bind(tracking.as_deref());
        q.bind(notes.as_deref());
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_order_items(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  order_items     ({count:>5}) ... ");
    let mut rng = rand::thread_rng();

    for _ in 0..count {
        let order_id: i64 = rng.gen_range(1..=2000);
        let product_id: i32 = rng.gen_range(1..=500);
        let qty: i16 = rng.gen_range(1..=20);
        let unit_price: f64 = rng.gen_range(99u32..=99_900) as f64 / 100.0;
        let discount_pct: f64 = match rng.gen_range(0u8..10) {
            0 => 5.0,
            1 => 10.0,
            2 => 15.0,
            3 => 20.0,
            _ => 0.0,
        };
        let line_total: f64 =
            (unit_price * qty as f64 * (1.0 - discount_pct / 100.0) * 100.0).round() / 100.0;
        let warehouse: Option<&str> =
            if rng.gen_bool(0.9) { Some(pick(WAREHOUSES, &mut rng)) } else { None };
        let notes: Option<&str> =
            if rng.gen_bool(0.05) { Some("handle with care") } else { None };

        let mut q = Query::new(
            "INSERT INTO dbo.order_items
             (order_id,product_id,quantity,unit_price,discount_pct,line_total,warehouse_code,notes)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8)",
        );
        q.bind(order_id);
        q.bind(product_id);
        q.bind(qty);
        q.bind(unit_price);
        q.bind(discount_pct);
        q.bind(line_total);
        q.bind(warehouse);
        q.bind(notes);
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_events(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  events          ({count:>5}) ... ");
    let mut rng = rand::thread_rng();
    let err_codes = ["ERR_001", "ERR_002", "ERR_DB", "ERR_TIMEOUT", "ERR_AUTH", "ERR_PERM"];

    for _ in 0..count {
        let event_type = pick(EVENT_TYPES, &mut rng);
        let source: Option<&str> =
            if rng.gen_bool(0.95) { Some(pick(SOURCE_SYSTEMS, &mut rng)) } else { None };
        let severity: u8 = match event_type {
            "ERROR" | "SYSTEM_ALERT" => rng.gen_range(3..=5),
            "WARNING" => rng.gen_range(2..=3),
            _ => rng.gen_range(0..=1),
        };
        let event_time = past_dt(365);
        let duration_ms: Option<i32> =
            if rng.gen_bool(0.85) { Some(rng.gen_range(1..=30_000)) } else { None };
        let user_id: Option<i32> =
            if rng.gen_bool(0.75) { Some(rng.gen_range(1..=1000)) } else { None };
        let session_id: Option<String> =
            if rng.gen_bool(0.7) { Some(Uuid::new_v4().to_string()) } else { None };
        let ip: Option<String> = Some(format!(
            "{}.{}.{}.{}",
            rng.gen_range(1u8..=254),
            rng.gen::<u8>(),
            rng.gen::<u8>(),
            rng.gen_range(1u8..=254)
        ));
        let words: Vec<String> = Words(1..3).fake();
        let payload = format!(
            r#"{{"event":"{}","ts":"{}","data":{{"key":"{}","value":{}}}}}"#,
            event_type,
            event_time.format("%Y-%m-%dT%H:%M:%SZ"),
            words.join("_"),
            rng.gen_range(0u32..=9999)
        );
        let (err_code, err_msg): (Option<&str>, Option<String>) = if event_type == "ERROR" {
            (Some(pick(&err_codes, &mut rng)), Some(Sentence(3..8).fake()))
        } else {
            (None, None)
        };
        let is_processed = rng.gen_bool(0.85);
        let processed_at: Option<NaiveDateTime> = if is_processed {
            Some(event_time + Duration::seconds(rng.gen_range(1..=3600)))
        } else {
            None
        };

        let mut q = Query::new(
            "INSERT INTO dbo.events
             (event_type,source_system,severity,event_time,duration_ms,user_id,session_id,
              ip_address,payload,error_code,error_message,is_processed,processed_at)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13)",
        );
        q.bind(event_type);
        q.bind(source);
        q.bind(severity);
        q.bind(event_time);
        q.bind(duration_ms);
        q.bind(user_id);
        q.bind(session_id.as_deref());
        q.bind(ip.as_deref());
        q.bind(payload.as_str());
        q.bind(err_code);
        q.bind(err_msg.as_deref());
        q.bind(is_processed);
        q.bind(processed_at);
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_measurements(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  measurements    ({count:>5}) ... ");
    let mut rng = rand::thread_rng();

    for _ in 0..count {
        let dtype = pick(DEVICE_TYPES, &mut rng);
        let sensor_id = format!("{}-{:04}", dtype, rng.gen_range(1u16..=100));
        let measured_at = past_dt(180);
        // REAL column → f32
        let temperature: Option<f32> =
            if rng.gen_bool(0.9) { Some(rng.gen_range(-400i32..=850) as f32 / 10.0) } else { None };
        // FLOAT column → f64
        let pressure: Option<f64> =
            if rng.gen_bool(0.75) { Some(rng.gen_range(9000..=11000) as f64 / 10.0) } else { None };
        // DECIMAL(5,2)
        let humidity: Option<f64> =
            if rng.gen_bool(0.8) { Some(rng.gen_range(0u32..=10000) as f64 / 100.0) } else { None };
        let voltage: Option<f64> =
            if rng.gen_bool(0.7) { Some(rng.gen_range(0u32..=5000) as f64 / 1000.0) } else { None };
        let current: Option<f64> =
            if rng.gen_bool(0.7) { Some(rng.gen_range(0u32..=10000) as f64 / 1000.0) } else { None };
        let power: Option<f64> = match (voltage, current) {
            (Some(v), Some(c)) => Some((v * c * 10000.0).round() / 10000.0),
            _ => None,
        };
        let lat: Option<f64> =
            if rng.gen_bool(0.5) { Some(rng.gen_range(-900_000i32..=900_000) as f64 / 10000.0) } else { None };
        let lon: Option<f64> =
            if rng.gen_bool(0.5) { Some(rng.gen_range(-1_800_000i32..=1_800_000) as f64 / 10000.0) } else { None };
        let altitude: Option<f64> =
            if rng.gen_bool(0.4) { Some(rng.gen_range(-100i32..=8848) as f64) } else { None };
        let signal: Option<i16> =
            if rng.gen_bool(0.85) { Some(rng.gen_range(-120i16..=0)) } else { None };
        // TINYINT → u8
        let battery: Option<u8> =
            if rng.gen_bool(0.9) { Some(rng.gen_range(0u8..=100)) } else { None };
        let raw_len = rng.gen_range(8usize..=64);
        let raw_bytes: Vec<u8> = (0..raw_len).map(|_| rng.gen()).collect();
        let checksum: Vec<u8> = (0..16).map(|_| rng.gen()).collect();

        let mut q = Query::new(
            "INSERT INTO dbo.measurements
             (sensor_id,device_type,measured_at,temperature_c,pressure_hpa,humidity_pct,
              voltage_v,current_a,power_watts,latitude,longitude,altitude_m,
              signal_strength,battery_pct,raw_bytes,checksum)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15,@P16)",
        );
        q.bind(sensor_id.as_str());
        q.bind(dtype);
        q.bind(measured_at);
        q.bind(temperature);
        q.bind(pressure);
        q.bind(humidity);
        q.bind(voltage);
        q.bind(current);
        q.bind(power);
        q.bind(lat);
        q.bind(lon);
        q.bind(altitude);
        q.bind(signal);
        q.bind(battery);
        q.bind(raw_bytes.as_slice());
        q.bind(checksum.as_slice());
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_transactions(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  transactions    ({count:>5}) ... ");
    let mut rng = rand::thread_rng();
    let channels = ["web", "mobile", "api", "pos", "atm"];

    for _ in 0..count {
        let account = format!("{:016}", rng.gen::<u64>());
        let trans_type = pick(TRANS_TYPES, &mut rng);
        // DECIMAL(18,4) → f64; SQL Server converts implicitly
        let amount: f64 = rng.gen_range(1u32..=1_000_000) as f64 / 100.0;
        let currency = pick(CURRENCIES, &mut rng);
        let exchange_rate: f64 = if currency == "USD" {
            1.0
        } else {
            rng.gen_range(50u32..=200) as f64 / 100.0
        };
        let amount_usd: f64 = (amount / exchange_rate * 100.0).round() / 100.0;
        let trans_dt = past_dt(365);
        let value_date: Option<NaiveDate> =
            Some((trans_dt + Duration::days(rng.gen_range(0i64..=3))).date());
        let reference = format!("REF{:012}", rng.gen::<u64>() % 1_000_000_000_000u64);
        let merchant: Option<String> =
            if trans_type == "PURCHASE" { Some(CompanyName().fake()) } else { None };
        let merchant_cat: Option<&str> =
            if merchant.is_some() { Some(pick(MERCHANT_CATS, &mut rng)) } else { None };
        let desc: String = Sentence(3..8).fake();
        let status = match rng.gen_range(0u8..10) {
            0 | 1 => "FAILED",
            2 => "REVERSED",
            _ => "SETTLED",
        };
        let is_reversed = status == "REVERSED";
        let batch_id = format!("BATCH-{}-{:04}", trans_dt.format("%Y%m%d"), rng.gen_range(1u16..=9999));
        let fingerprint = Uuid::new_v4().to_string();
        let channel = pick(&channels, &mut rng);
        let ip = format!(
            "{}.{}.{}.{}",
            rng.gen_range(1u8..=254), rng.gen::<u8>(), rng.gen::<u8>(), rng.gen_range(1u8..=254)
        );
        let metadata = format!(
            r#"{{"ip":"{}","channel":"{}","device_fingerprint":"{}"}}"#,
            ip, channel, fingerprint
        );

        let mut q = Query::new(
            "INSERT INTO sales.transactions
             (account_number,trans_type,amount,currency,exchange_rate,amount_usd,trans_datetime,
              value_date,reference_num,merchant_name,merchant_cat,description,status,
              is_reversed,batch_id,metadata)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15,@P16)",
        );
        q.bind(account.as_str());
        q.bind(trans_type);
        q.bind(amount);
        q.bind(currency);
        q.bind(exchange_rate);
        q.bind(amount_usd);
        q.bind(trans_dt);
        q.bind(opt_as_dt(value_date));
        q.bind(reference.as_str());
        q.bind(merchant.as_deref());
        q.bind(merchant_cat);
        q.bind(desc.as_str());
        q.bind(status);
        q.bind(is_reversed);
        q.bind(batch_id.as_str());
        q.bind(metadata.as_str());
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

async fn seed_employees(db: &mut MssqlClient, count: usize) -> Result<()> {
    print!("  hr.employees    ({count:>5}) ... ");
    let mut rng = rand::thread_rng();
    let job_titles = [
        "Software Engineer", "Senior Software Engineer", "Staff Engineer",
        "Product Manager", "Senior Product Manager", "Engineering Manager",
        "Data Analyst", "Data Scientist", "DevOps Engineer", "SRE",
        "Account Executive", "Sales Manager", "Marketing Specialist",
        "HR Business Partner", "Finance Analyst", "Legal Counsel",
    ];

    for i in 0..count {
        let first: String = FirstName().fake();
        let last: String = LastName().fake();
        let middle: Option<String> =
            if rng.gen_bool(0.4) { Some(FirstName().fake()) } else { None };
        // Ensure unique email by appending sequence number
        let email = format!(
            "{}.{}{}@example-corp.com",
            first.to_lowercase().replace(' ', "."),
            last.to_lowercase().replace(' ', "."),
            i + 1
        );
        let phone_work: Option<String> =
            if rng.gen_bool(0.9) {
                Some(format!("+1{}", rng.gen_range(2_000_000_000u64..=9_999_999_999u64)))
            } else {
                None
            };
        let phone_mobile: Option<String> =
            if rng.gen_bool(0.7) {
                Some(format!("+1{}", rng.gen_range(2_000_000_000u64..=9_999_999_999u64)))
            } else {
                None
            };
        let dept = pick(DEPARTMENTS, &mut rng);
        let title = pick(&job_titles, &mut rng);
        let job_level: u8 = rng.gen_range(1u8..=5);
        let manager_id: Option<i32> =
            if i > 0 && rng.gen_bool(0.9) { Some(rng.gen_range(1..=i as i32)) } else { None };
        let hire_date = past_date(3650);
        let termination_dt: Option<NaiveDate> = if rng.gen_bool(0.1) {
            let t = hire_date + Duration::days(rng.gen_range(180..=1800));
            if t <= Utc::now().date_naive() { Some(t) } else { None }
        } else {
            None
        };
        let salary: f64 = rng.gen_range(40_000u32..=250_000) as f64;
        let bonus_pct: Option<f64> =
            if rng.gen_bool(0.7) { Some(rng.gen_range(0u32..=30) as f64) } else { None };
        let is_full_time = rng.gen_bool(0.9);
        let office = format!(
            "{}, {} {}",
            CityName().fake::<String>(),
            StateAbbr().fake::<String>(),
            rng.gen_range(10000u32..99999)
        );
        let tz = pick(TIMEZONES, &mut rng);
        let bio: Option<String> =
            if rng.gen_bool(0.5) {
                Some(Sentences(2..4).fake::<Vec<String>>().join(" "))
            } else {
                None
            };
        let emp_code = format!("EMP-{:05}", i + 1);

        let mut q = Query::new(
            "INSERT INTO hr.employees
             (emp_code,first_name,last_name,middle_name,email,phone_work,phone_mobile,
              department,job_title,job_level,manager_id,hire_date,termination_dt,
              salary,bonus_pct,is_full_time,office_location,country_code,timezone,bio)
             VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,
                    @P14,@P15,@P16,@P17,@P18,@P19,@P20)",
        );
        q.bind(emp_code.as_str());
        q.bind(first.as_str());
        q.bind(last.as_str());
        q.bind(middle.as_deref());
        q.bind(email.as_str());
        q.bind(phone_work.as_deref());
        q.bind(phone_mobile.as_deref());
        q.bind(dept);
        q.bind(title);
        q.bind(job_level);
        q.bind(manager_id);
        q.bind(as_dt(hire_date));
        q.bind(opt_as_dt(termination_dt));
        q.bind(salary);
        q.bind(bonus_pct);
        q.bind(is_full_time);
        q.bind(office.as_str());
        q.bind("US");
        q.bind(tz);
        q.bind(bio.as_deref());
        q.execute(db).await?;
    }
    println!("done");
    Ok(())
}

// ─── entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    println!("╔══════════════════════════════════════════════╗");
    println!("║   StreamXfer  –  Test Data Seeder            ║");
    println!("╚══════════════════════════════════════════════╝\n");

    // ── Step 1: create database (connect to master) ──
    println!("[1/3] Creating database streamxfer_test …");
    {
        let mut master = connect(None).await?;
        create_database(&mut master).await?;
    }
    println!("  ✓ database ready\n");

    // ── Step 2: create schemas + tables ──
    println!("[2/3] Creating schemas and tables …");
    {
        let mut db = connect(Some("streamxfer_test")).await?;
        create_schemas(&mut db).await?;
        create_tables(&mut db).await?;
    }
    println!();

    // ── Step 3: seed data ──
    println!("[3/3] Inserting fake data …");
    {
        let mut db = connect(Some("streamxfer_test")).await?;
        seed_customers(&mut db, 1_000).await?;
        seed_products(&mut db, 500).await?;
        seed_orders(&mut db, 2_000).await?;
        seed_order_items(&mut db, 5_000).await?;
        seed_events(&mut db, 3_000).await?;
        seed_measurements(&mut db, 2_000).await?;
        seed_transactions(&mut db, 1_500).await?;
        seed_employees(&mut db, 300).await?;
    }

    println!("\n╔══════════════════════════════════════════════╗");
    println!("║   ✓  Seeding complete  ( ~30M rows)        ║");
    println!("╚══════════════════════════════════════════════╝");
    println!();
    println!("Connection string:");
    println!("  mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test");
    println!();
    println!("MinIO console:  http://localhost:9001  (minioadmin / minioadmin123)");
    println!("S3 endpoint:    http://localhost:9000");
    Ok(())
}
