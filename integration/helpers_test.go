//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// DSNs for the docker-compose databases.
const (
	// pgDSN is used by both pgx (archive) and lib/pq (seeding).
	pgDSN = "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable"

	// mysqlGoDSN is the Go MySQL driver format used by sql.Open and archive.
	mysqlGoDSN = "testuser:testpass@tcp(localhost:3306)/testdb?parseTime=true"
)

// verifyPostgres pings the local PostgreSQL instance.
func verifyPostgres() error {
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// verifyMySQL pings the local MySQL instance.
func verifyMySQL() error {
	db, err := sql.Open("mysql", mysqlGoDSN)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// seedPostgres creates and seeds a small inttest_orders table (drops + recreates each run).
func seedPostgres() error {
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(`DROP TABLE IF EXISTS inttest_orders`); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE inttest_orders (
			id SERIAL PRIMARY KEY,
			customer_name TEXT NOT NULL,
			amount DECIMAL(10, 2),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO inttest_orders (customer_name, amount, created_at) VALUES
			('Zara', 980.00, NOW() - INTERVAL '4 years'),
			('Yuri', 430.50, NOW() - INTERVAL '3 years'),
			('Xena', 125.75, NOW() - INTERVAL '2 years'),
			('Wade', 310.00, NOW() - INTERVAL '20 months'),
			('Alice', 120.50, NOW() - INTERVAL '6 months'),
			('Bob', 89.99, NOW() - INTERVAL '4 months'),
			('Charlie', 250.00, NOW() - INTERVAL '2 months'),
			('Diana', 310.25, NOW() - INTERVAL '1 month')
	`)
	if err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	return nil
}

// seedPostgresAllTypes creates inttest_alltypes with diverse PostgreSQL column types.
func seedPostgresAllTypes() error {
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	// Drop enum type and table.
	if _, err := db.Exec(`DROP TABLE IF EXISTS inttest_alltypes`); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	if _, err := db.Exec(`DROP TYPE IF EXISTS inttest_size_enum CASCADE`); err != nil {
		return fmt.Errorf("drop enum: %w", err)
	}

	if _, err := db.Exec(`CREATE TYPE inttest_size_enum AS ENUM ('small', 'medium', 'large', 'xl')`); err != nil {
		return fmt.Errorf("create enum: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE inttest_alltypes (
			id              SERIAL PRIMARY KEY,
			col_smallint    SMALLINT,
			col_integer     INTEGER,
			col_bigint      BIGINT,
			col_real        REAL,
			col_double      DOUBLE PRECISION,
			col_numeric     NUMERIC(12, 4),
			col_boolean     BOOLEAN,
			col_char        CHAR(10),
			col_varchar     VARCHAR(255),
			col_text        TEXT,
			col_bytea       BYTEA,
			col_date        DATE,
			col_time        TIME,
			col_timestamp   TIMESTAMP,
			col_timestamptz TIMESTAMPTZ,
			col_uuid        UUID,
			col_json        JSON,
			col_jsonb       JSONB,
			col_int_array   INTEGER[],
			col_text_array  TEXT[],
			col_inet        INET,
			col_enum        inttest_size_enum,
			col_nullable    TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// 10 rows covering typical values, edge cases, NULLs, unicode, etc.
	rows := []string{
		// Row 1: all populated with typical values
		`(1, 100, 50000, 9000000000, 3.14, 2.718281828459045, 12345678.1234, true,
		  'ABCDEFGHIJ', 'Hello, World!', 'Typical text value.',
		  E'\\x48656C6C6F', '2024-06-15', '14:30:00', '2024-06-15 10:30:00', '2024-06-15 10:30:00+00',
		  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		  '{"name": "Alice", "age": 30}', '{"tags": ["important"], "priority": 1}',
		  '{1,2,3,4,5}', '{"red","green","blue"}', '192.168.1.1', 'medium', 'not null')`,
		// Row 2: negative values
		`(2, -100, -50000, -9000000000, -1.5, -0.001, -99999999.9999, false,
		  'ZYX       ', 'Another varchar', 'Different text.',
		  E'\\xCAFEBABE', '2024-02-15', '23:59:59', '2024-02-15 23:59:59', '2024-02-15 00:00:00+05:30',
		  'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22',
		  '{"items": [1, 2, 3]}', '{"active": false}',
		  '{10,20,30}', '{"alpha","beta"}', '10.0.0.1', 'large', 'another value')`,
		// Row 3: max integer values
		`(3, 32767, 2147483647, 9223372036854775807, 3.4028235e+38, 1.7976931348623157e+308, 99999999.9999, true,
		  '          ', '', '',
		  E'\\x00', '2024-08-01', '00:00:00', '2024-08-01 23:59:59', '2024-08-01 23:59:59+00',
		  '00000000-0000-0000-0000-000000000000',
		  '{}', '[]',
		  '{}', '{}', '255.255.255.255', 'xl', '')`,
		// Row 4: min integer values
		`(4, -32768, -2147483648, -9223372036854775808, -3.4028235e+38, -1.7976931348623157e+308, -99999999.9999, false,
		  'min       ', 'min values', 'Minimum boundary values.',
		  E'\\xFF', '2024-09-01', '00:00:00', '2024-09-01 00:00:00', '2024-09-01 00:00:00+00',
		  'ffffffff-ffff-ffff-ffff-ffffffffffff',
		  '{"min": true}', '{"min": true}',
		  '{-2147483648,0,2147483647}', '{""}', '0.0.0.0', 'small', 'min row')`,
		// Row 5: unicode, complex JSON
		`(5, 42, 12345, 9876543210, 1.618, 3.141592653589793, 1234.5678, true,
		  'Unicode   ', 'Cafe au lait', 'Accents and special chars.',
		  E'\\xF09F988A', '2024-01-01', '12:00:00', '2024-07-04 09:00:00', '2024-07-04 09:00:00-04',
		  'c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33',
		  '{"address": {"city": "Tokyo"}, "phones": ["+81-3-1234"]}',
		  '{"en": "hello", "ja": "konnichiwa"}',
		  '{100,200,300}', '{"Tokyo","London"}', '::1', 'medium', 'unicode row')`,
		// Row 6: some NULLs
		`(6, NULL, 999, NULL, NULL, 1.0, NULL, NULL,
		  NULL, 'partial', NULL,
		  NULL, '2024-03-15', NULL, '2024-03-15 08:00:00', NULL,
		  NULL, NULL, '{"sparse": true}',
		  NULL, '{"only-text"}', NULL, 'small', NULL)`,
		// Row 7: many NULLs
		`(7, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		  NULL, NULL, 'Only text and enum.',
		  NULL, NULL, NULL, NULL, NULL,
		  NULL, NULL, NULL,
		  NULL, NULL, NULL, 'large', NULL)`,
		// Row 8: zero/false/empty
		`(8, 0, 0, 0, 0.0, 0.0, 0.0000, false,
		  '0000000000', '0', '0',
		  E'\\x00000000', '2024-10-01', '00:00:00', '2024-10-01 00:00:00', '2024-10-01 00:00:00+00',
		  '00000000-0000-0000-0000-000000000000',
		  '{"zero": 0, "false": false, "null_val": null}', '{"zero": 0}',
		  '{0}', '{""}', '0.0.0.0', 'small', '0')`,
		// Row 9: different date values
		`(9, 1, 1, 1, 0.1, 0.1, 0.0001, true,
		  'epoch     ', 'epoch dates', 'Date boundaries.',
		  E'\\x01020304', '2024-11-01', '12:34:56', '2024-11-01 00:00:00', '2024-11-01 00:00:00+00',
		  'd4eebc99-9c0b-4ef8-bb6d-6bb9bd380a44',
		  '{"epoch": true}', '{"epoch": true}',
		  '{1}', '{"epoch"}', '127.0.0.1', 'xl', 'epoch row')`,
		// Row 10: all NULLs except id
		`(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		  NULL, NULL, NULL,
		  NULL, NULL, NULL, NULL, NULL,
		  NULL, NULL, NULL,
		  NULL, NULL, NULL, NULL, NULL)`,
	}

	for _, row := range rows {
		if _, err := db.Exec(`INSERT INTO inttest_alltypes VALUES ` + row); err != nil {
			return fmt.Errorf("seed row: %w", err)
		}
	}
	return nil
}

// seedMySQL creates and seeds a small inttest_orders table (drops + recreates each run).
func seedMySQL() error {
	db, err := sql.Open("mysql", mysqlGoDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(`DROP TABLE IF EXISTS inttest_orders`); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE inttest_orders (
			id INT AUTO_INCREMENT PRIMARY KEY,
			customer_name VARCHAR(100),
			amount DECIMAL(10,2),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO inttest_orders (customer_name, amount, created_at) VALUES
			('Zara', 980.00, DATE_SUB(NOW(), INTERVAL 48 MONTH)),
			('Yuri', 430.50, DATE_SUB(NOW(), INTERVAL 36 MONTH)),
			('Xena', 125.75, DATE_SUB(NOW(), INTERVAL 24 MONTH)),
			('Wade', 310.00, DATE_SUB(NOW(), INTERVAL 20 MONTH)),
			('Alice', 120.50, DATE_SUB(NOW(), INTERVAL 6 MONTH)),
			('Bob', 89.99, DATE_SUB(NOW(), INTERVAL 4 MONTH)),
			('Charlie', 250.00, DATE_SUB(NOW(), INTERVAL 2 MONTH)),
			('Diana', 310.25, DATE_SUB(NOW(), INTERVAL 1 MONTH))
	`)
	if err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	return nil
}

// seedMySQLAllTypes creates inttest_alltypes with diverse MySQL column types.
func seedMySQLAllTypes() error {
	db, err := sql.Open("mysql", mysqlGoDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(`DROP TABLE IF EXISTS inttest_alltypes`); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE inttest_alltypes (
			id              INT AUTO_INCREMENT PRIMARY KEY,
			col_tinyint     TINYINT,
			col_smallint    SMALLINT,
			col_mediumint   MEDIUMINT,
			col_integer     INT,
			col_bigint      BIGINT,
			col_float       FLOAT,
			col_double      DOUBLE,
			col_decimal     DECIMAL(12, 4),
			col_boolean     BOOLEAN,
			col_char        CHAR(10),
			col_varchar     VARCHAR(255),
			col_text        TEXT,
			col_mediumtext  MEDIUMTEXT,
			col_blob        BLOB,
			col_binary      BINARY(16),
			col_date        DATE,
			col_time        TIME,
			col_datetime    DATETIME,
			col_timestamp   TIMESTAMP NULL,
			col_json        JSON,
			col_enum        ENUM('small', 'medium', 'large', 'xl'),
			col_set         SET('read', 'write', 'execute'),
			col_nullable    TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	rows := []string{
		// Row 1: typical values
		`(1, 127, 100, 50000, 1000000, 9000000000, 3.14, 2.718281828459045, 12345678.1234, true,
		  'ABCDEFGHIJ', 'Hello, World!', 'Typical text.', 'Medium text.',
		  X'48656C6C6F', X'0102030405060708090A0B0C0D0E0F10',
		  '2024-06-15', '14:30:00', '2024-06-15 10:30:00', '2024-06-15 10:30:00',
		  '{"name": "Alice", "age": 30}', 'medium', 'read,write', 'not null')`,
		// Row 2: negative values
		`(2, -128, -100, -50000, -1000000, -9000000000, -1.5, -0.001, -99999999.9999, false,
		  'ZYX       ', 'Another varchar', 'Different text.', 'More medium text.',
		  X'CAFEBABE', X'FF00FF00FF00FF00FF00FF00FF00FF00',
		  '2024-02-15', '23:59:59', '2024-02-15 23:59:59', '2024-02-15 00:00:00',
		  '{"items": [1, 2, 3]}', 'large', 'read,write,execute', 'another value')`,
		// Row 3: max integer values
		`(3, 127, 32767, 8388607, 2147483647, 9223372036854775807, 1.0e+38, 1.7976931348623157e+308, 99999999.9999, true,
		  '          ', '', '', '',
		  X'00', X'00000000000000000000000000000000',
		  '2024-08-01', '00:00:00', '2024-08-01 23:59:59', '2024-08-01 23:59:59',
		  '{}', 'xl', 'execute', '')`,
		// Row 4: min integer values
		`(4, -128, -32768, -8388608, -2147483648, -9223372036854775808, -1.0e+38, -1.7976931348623157e+308, -99999999.9999, false,
		  'min       ', 'min values', 'Minimum boundaries.', 'Min medium text.',
		  X'FF', X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
		  '2024-09-01', '00:00:00', '2024-09-01 00:00:00', '2024-09-01 00:00:01',
		  '{"min": true}', 'small', 'read', 'min row')`,
		// Row 5: unicode, complex JSON
		`(5, 42, 42, 12345, 12345, 9876543210, 1.618, 3.141592653589793, 1234.5678, true,
		  'Unicode   ', 'Cafe au lait', 'Accents and special.', 'Unicode medium text.',
		  X'F09F988A', X'F09F988AF09F988AF09F988A00000000',
		  '2024-01-01', '12:00:00', '2024-07-04 09:00:00', '2024-07-04 13:00:00',
		  '{"address": {"city": "Tokyo"}, "phones": ["+81-3-1234"]}', 'medium', 'read,execute', 'unicode row')`,
		// Row 6: some NULLs
		`(6, NULL, 99, NULL, 999, NULL, NULL, 1.0, NULL, NULL,
		  NULL, 'partial', NULL, NULL,
		  NULL, NULL,
		  '2024-03-15', NULL, '2024-03-15 08:00:00', NULL,
		  NULL, 'small', NULL, NULL)`,
		// Row 7: many NULLs
		`(7, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		  NULL, NULL, 'Only text and enum.', NULL,
		  NULL, NULL,
		  NULL, NULL, NULL, NULL,
		  NULL, 'large', NULL, NULL)`,
		// Row 8: zero/false/empty
		`(8, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0000, false,
		  '0000000000', '0', '0', '0',
		  X'00000000', X'00000000000000000000000000000000',
		  '2024-10-01', '00:00:00', '2024-10-01 00:00:00', '2024-10-01 00:00:00',
		  '{"zero": 0, "false": false, "null_val": null}', 'small', 'read', '0')`,
		// Row 9: different date values
		`(9, 1, 1, 1, 1, 1, 0.1, 0.1, 0.0001, true,
		  'epoch     ', 'epoch dates', 'Date boundaries.', 'Epoch medium text.',
		  X'0102030405060708', X'01020304050607080102030405060708',
		  '2024-11-01', '12:34:56', '2024-11-01 00:00:00', '2024-11-01 00:00:01',
		  '{"epoch": true}', 'xl', 'write,execute', 'epoch row')`,
		// Row 10: all NULLs except id
		`(10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		  NULL, NULL, NULL, NULL,
		  NULL, NULL,
		  NULL, NULL, NULL, NULL,
		  NULL, NULL, NULL, NULL)`,
	}

	for _, row := range rows {
		if _, err := db.Exec(`INSERT INTO inttest_alltypes VALUES ` + row); err != nil {
			return fmt.Errorf("seed row: %w", err)
		}
	}
	return nil
}
