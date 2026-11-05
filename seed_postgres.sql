-- ---------------------------------------------------------------------------
-- ColdPlane PostgreSQL seed — comprehensive type coverage
-- Runs once on first docker-compose up (via /docker-entrypoint-initdb.d/)
-- To re-seed: docker compose down -v && docker compose up -d
-- ---------------------------------------------------------------------------

-- =========================================================================
-- Table 1: orders — basic types for quick manual testing
-- =========================================================================

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    amount        DECIMAL(10, 2),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orders (customer_name, amount, created_at) VALUES
    -- Stale data (>18 months old) — for analyze command testing
    ('Zara Olson',      980.00, NOW() - INTERVAL '4 years'),
    ('Yuri Petrov',     430.50, NOW() - INTERVAL '3 years 6 months'),
    ('Xena Walters',    125.75, NOW() - INTERVAL '3 years'),
    ('Wade Thomas',     310.00, NOW() - INTERVAL '2 years 6 months'),
    ('Vera Santos',     875.25, NOW() - INTERVAL '2 years 3 months'),
    ('Uma Patel',        55.00, NOW() - INTERVAL '2 years'),
    ('Tony Reeves',     640.90, NOW() - INTERVAL '22 months'),
    ('Sara Kim',        215.30, NOW() - INTERVAL '20 months'),
    ('Ray Johnson',     770.00, NOW() - INTERVAL '19 months'),
    ('Pat Morgan',       99.99, NOW() - INTERVAL '18 months 1 day'),
    -- Recent data (<18 months old)
    ('Alice Johnson',   120.50, NOW() - INTERVAL '11 months'),
    ('Bob Smith',        89.99, NOW() - INTERVAL '10 months'),
    ('Charlie Brown',   250.00, NOW() - INTERVAL '9 months'),
    ('Diana Prince',    310.25, NOW() - INTERVAL '8 months'),
    ('Eve Davis',        45.00, NOW() - INTERVAL '7 months'),
    ('Frank Miller',    780.10, NOW() - INTERVAL '6 months'),
    ('Grace Hopper',    199.99, NOW() - INTERVAL '5 months'),
    ('Hank Pym',         62.30, NOW() - INTERVAL '4 months'),
    ('Ivy Chen',        410.75, NOW() - INTERVAL '3 months'),
    ('Jack Wilson',      33.33, NOW() - INTERVAL '2 months'),
    ('Karen Lee',       555.55, NOW() - INTERVAL '6 weeks'),
    ('Leo Garcia',      101.01, NOW() - INTERVAL '5 weeks'),
    ('Mia Taylor',      875.00, NOW() - INTERVAL '4 weeks'),
    ('Noah White',       19.99, NOW() - INTERVAL '3 weeks'),
    ('Olivia Harris',   640.20, NOW() - INTERVAL '2 weeks'),
    ('Paul Clark',      222.22, NOW() - INTERVAL '10 days'),
    ('Quinn Adams',      99.95, NOW() - INTERVAL '7 days'),
    ('Rita Moreno',     350.00, NOW() - INTERVAL '3 days'),
    ('Sam Turner',      175.80, NOW() - INTERVAL '1 day'),
    ('Tina Brooks',      50.00, NOW() - INTERVAL '1 hour');

-- =========================================================================
-- Table 2: alltypes — comprehensive PostgreSQL type coverage
-- =========================================================================

DROP TYPE IF EXISTS size_enum CASCADE;
CREATE TYPE size_enum AS ENUM ('small', 'medium', 'large', 'xl');

DROP TABLE IF EXISTS alltypes;
CREATE TABLE alltypes (
    id              SERIAL PRIMARY KEY,
    -- Integer types
    col_smallint    SMALLINT,
    col_integer     INTEGER,
    col_bigint      BIGINT,
    -- Floating point
    col_real        REAL,
    col_double      DOUBLE PRECISION,
    col_numeric     NUMERIC(12, 4),
    -- Boolean
    col_boolean     BOOLEAN,
    -- Character types
    col_char        CHAR(10),
    col_varchar     VARCHAR(255),
    col_text        TEXT,
    -- Binary
    col_bytea       BYTEA,
    -- Date/Time
    col_date        DATE,
    col_time        TIME,
    col_timestamp   TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    -- UUID
    col_uuid        UUID,
    -- JSON
    col_json        JSON,
    col_jsonb       JSONB,
    -- Arrays
    col_int_array   INTEGER[],
    col_text_array  TEXT[],
    -- Network
    col_inet        INET,
    -- Enum
    col_enum        size_enum,
    -- Nullable (some rows NULL, some not)
    col_nullable    TEXT
);

-- Row 1: all columns populated with typical values
INSERT INTO alltypes VALUES (
    1,
    100, 50000, 9000000000,
    3.14, 2.718281828459045, 12345678.1234,
    true,
    'ABCDEFGHIJ', 'Hello, World!', 'This is a typical text value with some content.',
    E'\\x48656C6C6F',
    '2024-06-15', '14:30:00', '2024-06-15 10:30:00', '2024-06-15 10:30:00+00',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{"name": "Alice", "age": 30}',
    '{"tags": ["important", "urgent"], "priority": 1}',
    '{1, 2, 3, 4, 5}', '{"red", "green", "blue"}',
    '192.168.1.1',
    'medium',
    'not null value'
);

-- Row 2: different typical values
INSERT INTO alltypes VALUES (
    2,
    -100, -50000, -9000000000,
    -1.5, -0.001, -99999999.9999,
    false,
    'ZYX       ', 'Another varchar value', 'Different text content here.',
    E'\\xCAFEBABE',
    '2024-02-15', '23:59:59', '2024-02-15 23:59:59', '2024-02-15 00:00:00+05:30',
    'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22',
    '{"items": [1, 2, 3], "nested": {"key": "val"}}',
    '{"active": false, "count": 0}',
    '{10, 20, 30}', '{"alpha", "beta"}',
    '10.0.0.1',
    'large',
    'another non-null'
);

-- Row 3: max integer values, empty string edge cases
INSERT INTO alltypes VALUES (
    3,
    32767, 2147483647, 9223372036854775807,
    3.4028235e+38, 1.7976931348623157e+308, 99999999.9999,
    true,
    '          ', '', '',
    E'\\x00',
    '2024-08-01', '00:00:00', '2024-08-01 23:59:59', '2024-08-01 23:59:59+00',
    '00000000-0000-0000-0000-000000000000',
    '{}', '[]',
    '{}', '{}',
    '255.255.255.255',
    'xl',
    ''
);

-- Row 4: min integer values, negative extremes
INSERT INTO alltypes VALUES (
    4,
    -32768, -2147483648, -9223372036854775808,
    -3.4028235e+38, -1.7976931348623157e+308, -99999999.9999,
    false,
    'min       ', 'min values row', 'Testing minimum and negative boundary values.',
    E'\\xFF',
    '2024-09-01', '00:00:00', '2024-09-01 00:00:00', '2024-09-01 00:00:00+00',
    'ffffffff-ffff-ffff-ffff-ffffffffffff',
    '{"min": true}', '{"min": true}',
    '{-2147483648, 0, 2147483647}', '{""}',
    '0.0.0.0',
    'small',
    'min row'
);

-- Row 5: unicode text, complex nested JSON
INSERT INTO alltypes VALUES (
    5,
    42, 12345, 9876543210,
    1.618, 3.141592653589793, 1234.5678,
    true,
    'Unicode   ', 'Caf e au lait', 'Emojis work too. Accents: resume. CJK: characters.',
    E'\\xF09F988A',
    '2024-01-01', '12:00:00', '2024-07-04 09:00:00', '2024-07-04 09:00:00-04',
    'c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33',
    '{"address": {"street": "123 Main St", "city": "Tokyo", "zip": "100-0001"}, "phones": ["+81-3-1234-5678"]}',
    '{"multilingual": {"en": "hello", "ja": "konnichiwa", "es": "hola"}}',
    '{100, 200, 300, 400}', '{"Tokyo", "New York", "London"}',
    '::1',
    'medium',
    'Unicode value with special chars'
);

-- Row 6: some NULLs (sparse row)
INSERT INTO alltypes VALUES (
    6,
    NULL, 999, NULL,
    NULL, 1.0, NULL,
    NULL,
    NULL, 'partial data', NULL,
    NULL,
    '2024-03-15', NULL, '2024-03-15 08:00:00', NULL,
    NULL,
    NULL, '{"sparse": true}',
    NULL, '{"only-text"}',
    NULL,
    'small',
    NULL
);

-- Row 7: many NULLs (mostly-null row)
INSERT INTO alltypes VALUES (
    7,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL,
    NULL, NULL, 'Only this text and the enum are non-null',
    NULL,
    NULL, NULL, NULL, NULL,
    NULL,
    NULL, NULL,
    NULL, NULL,
    NULL,
    'large',
    NULL
);

-- Row 8: zero/false/empty values (not NULL)
INSERT INTO alltypes VALUES (
    8,
    0, 0, 0,
    0.0, 0.0, 0.0000,
    false,
    '0000000000', '0', '0',
    E'\\x00000000',
    '2024-10-01', '00:00:00', '2024-10-01 00:00:00', '2024-10-01 00:00:00+00',
    '00000000-0000-0000-0000-000000000000',
    '{"zero": 0, "empty": "", "false": false, "null_val": null}',
    '{"zero": 0}',
    '{0}', '{""}',
    '0.0.0.0',
    'small',
    '0'
);

-- Row 9: edge dates (epoch, far future)
INSERT INTO alltypes VALUES (
    9,
    1, 1, 1,
    0.1, 0.1, 0.0001,
    true,
    'epoch     ', 'epoch and future dates', 'Testing date boundaries.',
    E'\\x0102030405060708',
    '2024-11-01', '12:34:56', '2024-11-01 00:00:00', '2024-11-01 00:00:00+00',
    'd4eebc99-9c0b-4ef8-bb6d-6bb9bd380a44',
    '{"epoch": "1970-01-01T00:00:00Z"}',
    '{"future": "2099-12-31"}',
    '{1}', '{"epoch"}',
    '127.0.0.1',
    'xl',
    'epoch row'
);

-- Row 10: all NULLs except id
INSERT INTO alltypes (id) VALUES (10);
