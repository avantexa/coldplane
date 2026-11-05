-- ---------------------------------------------------------------------------
-- ColdPlane MySQL seed — comprehensive type coverage
-- Runs once on first docker-compose up (via /docker-entrypoint-initdb.d/)
-- To re-seed: docker compose down -v && docker compose up -d
-- ---------------------------------------------------------------------------

-- =========================================================================
-- Table 1: orders — basic types for quick manual testing
-- =========================================================================

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    amount        DECIMAL(10, 2),
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (customer_name, amount, created_at) VALUES
    -- Stale data (>18 months old) — for analyze command testing
    ('Zara Olson',      980.00, DATE_SUB(NOW(), INTERVAL 48 MONTH)),
    ('Yuri Petrov',     430.50, DATE_SUB(NOW(), INTERVAL 42 MONTH)),
    ('Xena Walters',    125.75, DATE_SUB(NOW(), INTERVAL 36 MONTH)),
    ('Wade Thomas',     310.00, DATE_SUB(NOW(), INTERVAL 30 MONTH)),
    ('Vera Santos',     875.25, DATE_SUB(NOW(), INTERVAL 27 MONTH)),
    ('Uma Patel',        55.00, DATE_SUB(NOW(), INTERVAL 24 MONTH)),
    ('Tony Reeves',     640.90, DATE_SUB(NOW(), INTERVAL 22 MONTH)),
    ('Sara Kim',        215.30, DATE_SUB(NOW(), INTERVAL 20 MONTH)),
    ('Ray Johnson',     770.00, DATE_SUB(NOW(), INTERVAL 19 MONTH)),
    ('Pat Morgan',       99.99, DATE_SUB(NOW(), INTERVAL 19 MONTH)),
    -- Recent data (<18 months old)
    ('Alice Johnson',   120.50, DATE_SUB(NOW(), INTERVAL 11 MONTH)),
    ('Bob Smith',        89.99, DATE_SUB(NOW(), INTERVAL 10 MONTH)),
    ('Charlie Brown',   250.00, DATE_SUB(NOW(), INTERVAL 9 MONTH)),
    ('Diana Prince',    310.25, DATE_SUB(NOW(), INTERVAL 8 MONTH)),
    ('Eve Davis',        45.00, DATE_SUB(NOW(), INTERVAL 7 MONTH)),
    ('Frank Miller',    780.10, DATE_SUB(NOW(), INTERVAL 6 MONTH)),
    ('Grace Hopper',    199.99, DATE_SUB(NOW(), INTERVAL 5 MONTH)),
    ('Hank Pym',         62.30, DATE_SUB(NOW(), INTERVAL 4 MONTH)),
    ('Ivy Chen',        410.75, DATE_SUB(NOW(), INTERVAL 3 MONTH)),
    ('Jack Wilson',      33.33, DATE_SUB(NOW(), INTERVAL 2 MONTH)),
    ('Karen Lee',       555.55, DATE_SUB(NOW(), INTERVAL 6 WEEK)),
    ('Leo Garcia',      101.01, DATE_SUB(NOW(), INTERVAL 5 WEEK)),
    ('Mia Taylor',      875.00, DATE_SUB(NOW(), INTERVAL 4 WEEK)),
    ('Noah White',       19.99, DATE_SUB(NOW(), INTERVAL 3 WEEK)),
    ('Olivia Harris',   640.20, DATE_SUB(NOW(), INTERVAL 2 WEEK)),
    ('Paul Clark',      222.22, DATE_SUB(NOW(), INTERVAL 10 DAY)),
    ('Quinn Adams',      99.95, DATE_SUB(NOW(), INTERVAL 7 DAY)),
    ('Rita Moreno',     350.00, DATE_SUB(NOW(), INTERVAL 3 DAY)),
    ('Sam Turner',      175.80, DATE_SUB(NOW(), INTERVAL 1 DAY)),
    ('Tina Brooks',      50.00, DATE_SUB(NOW(), INTERVAL 1 HOUR));

-- =========================================================================
-- Table 2: alltypes — comprehensive MySQL type coverage
-- =========================================================================

DROP TABLE IF EXISTS alltypes;
CREATE TABLE alltypes (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    -- Integer types (MySQL-specific sizes)
    col_tinyint     TINYINT,
    col_smallint    SMALLINT,
    col_mediumint   MEDIUMINT,
    col_integer     INT,
    col_bigint      BIGINT,
    -- Floating point
    col_float       FLOAT,
    col_double      DOUBLE,
    col_decimal     DECIMAL(12, 4),
    -- Boolean
    col_boolean     BOOLEAN,
    -- Character types
    col_char        CHAR(10),
    col_varchar     VARCHAR(255),
    col_text        TEXT,
    col_mediumtext  MEDIUMTEXT,
    -- Binary
    col_blob        BLOB,
    col_binary      BINARY(16),
    -- Date/Time
    col_date        DATE,
    col_time        TIME,
    col_datetime    DATETIME,
    col_timestamp   TIMESTAMP NULL,
    -- JSON
    col_json        JSON,
    -- Enum and Set (MySQL-specific)
    col_enum        ENUM('small', 'medium', 'large', 'xl'),
    col_set         SET('read', 'write', 'execute'),
    -- Nullable (some rows NULL, some not)
    col_nullable    TEXT
);

-- Row 1: all columns populated with typical values
INSERT INTO alltypes VALUES (
    1,
    127, 100, 50000, 1000000, 9000000000,
    3.14, 2.718281828459045, 12345678.1234,
    true,
    'ABCDEFGHIJ', 'Hello, World!', 'This is a typical text value.', 'Medium text content here.',
    X'48656C6C6F', X'0102030405060708090A0B0C0D0E0F10',
    '2024-06-15', '14:30:00', '2024-06-15 10:30:00', '2024-06-15 10:30:00',
    '{"name": "Alice", "age": 30}',
    'medium', 'read,write',
    'not null value'
);

-- Row 2: different typical values, negative numbers
INSERT INTO alltypes VALUES (
    2,
    -128, -100, -50000, -1000000, -9000000000,
    -1.5, -0.001, -99999999.9999,
    false,
    'ZYX       ', 'Another varchar', 'Different text.', 'More medium text.',
    X'CAFEBABE', X'FF00FF00FF00FF00FF00FF00FF00FF00',
    '2024-02-15', '23:59:59', '2024-02-15 23:59:59', '2024-02-15 00:00:00',
    '{"items": [1, 2, 3], "nested": {"key": "val"}}',
    'large', 'read,write,execute',
    'another non-null'
);

-- Row 3: max integer values, empty strings
INSERT INTO alltypes VALUES (
    3,
    127, 32767, 8388607, 2147483647, 9223372036854775807,
    1.0e+38, 1.7976931348623157e+308, 99999999.9999,
    true,
    '          ', '', '', '',
    X'00', X'00000000000000000000000000000000',
    '2024-08-01', '00:00:00', '2024-08-01 23:59:59', '2024-08-01 23:59:59',
    '{}',
    'xl', 'execute',
    ''
);

-- Row 4: min integer values
INSERT INTO alltypes VALUES (
    4,
    -128, -32768, -8388608, -2147483648, -9223372036854775808,
    -1.0e+38, -1.7976931348623157e+308, -99999999.9999,
    false,
    'min       ', 'min values row', 'Testing minimum boundary values.', 'Minimum boundary medium text.',
    X'FF', X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    '2024-09-01', '00:00:00', '2024-09-01 00:00:00', '2024-09-01 00:00:01',
    '{"min": true}',
    'small', 'read',
    'min row'
);

-- Row 5: unicode text, complex nested JSON
INSERT INTO alltypes VALUES (
    5,
    42, 42, 12345, 12345, 9876543210,
    1.618, 3.141592653589793, 1234.5678,
    true,
    'Unicode   ', 'Cafe au lait', 'Emojis and accents: resume, naive.', 'CJK characters and more unicode.',
    X'F09F988A', X'F09F988AF09F988AF09F988A00000000',
    '2024-01-01', '12:00:00', '2024-07-04 09:00:00', '2024-07-04 13:00:00',
    '{"address": {"street": "123 Main St", "city": "Tokyo", "zip": "100-0001"}, "phones": ["+81-3-1234-5678"]}',
    'medium', 'read,execute',
    'Unicode value'
);

-- Row 6: some NULLs (sparse row)
INSERT INTO alltypes VALUES (
    6,
    NULL, 99, NULL, 999, NULL,
    NULL, 1.0, NULL,
    NULL,
    NULL, 'partial data', NULL, NULL,
    NULL, NULL,
    '2024-03-15', NULL, '2024-03-15 08:00:00', NULL,
    NULL,
    'small', NULL,
    NULL
);

-- Row 7: many NULLs (mostly-null row)
INSERT INTO alltypes VALUES (
    7,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL,
    NULL, NULL, 'Only this text and the enum are non-null', NULL,
    NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL,
    'large', NULL,
    NULL
);

-- Row 8: zero/false/empty values (not NULL)
INSERT INTO alltypes VALUES (
    8,
    0, 0, 0, 0, 0,
    0.0, 0.0, 0.0000,
    false,
    '0000000000', '0', '0', '0',
    X'00000000', X'00000000000000000000000000000000',
    '2024-10-01', '00:00:00', '2024-10-01 00:00:00', '2024-10-01 00:00:00',
    '{"zero": 0, "empty": "", "false": false, "null_val": null}',
    'small', 'read',
    '0'
);

-- Row 9: epoch and boundary dates
INSERT INTO alltypes VALUES (
    9,
    1, 1, 1, 1, 1,
    0.1, 0.1, 0.0001,
    true,
    'epoch     ', 'epoch and boundary dates', 'Testing date boundaries.', 'More boundary testing.',
    X'0102030405060708', X'01020304050607080102030405060708',
    '2024-11-01', '12:34:56', '2024-11-01 00:00:00', '2024-11-01 00:00:01',
    '{"epoch": "1970-01-01T00:00:00Z"}',
    'xl', 'write,execute',
    'epoch row'
);

-- Row 10: all NULLs except id
INSERT INTO alltypes (id) VALUES (10);
