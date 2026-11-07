package archive

import (
	"strings"
	"testing"
)

func TestPurgeBatchSQL(t *testing.T) {
	got := purgeBatchSQL("postgres", "orders", "amount > 0", 500)
	want := `DELETE FROM "orders" WHERE ctid IN (SELECT ctid FROM "orders" WHERE amount > 0 LIMIT 500)`
	if got != want {
		t.Errorf("pg:\n got  %s\n want %s", got, want)
	}
	got = purgeBatchSQL("postgres", "orders", "", 500)
	if got != `DELETE FROM "orders" WHERE ctid IN (SELECT ctid FROM "orders" LIMIT 500)` {
		t.Errorf("pg no where: %s", got)
	}
	got = purgeBatchSQL("mysql", "orders", "amount > 0", 500)
	if got != "DELETE FROM `orders` WHERE amount > 0 LIMIT 500" {
		t.Errorf("mysql: %s", got)
	}
	got = purgeBatchSQL("mysql", "orders", "", 500)
	if got != "DELETE FROM `orders` LIMIT 500" {
		t.Errorf("mysql no where: %s", got)
	}
}

func TestPurgeRefusal(t *testing.T) {
	ok := Manifest{DBName: "shop", VerifiedAt: "2026-01-01T00:00:00Z", Tables: []TableEntry{{Name: "t", Verified: &TableVerification{Status: verifyPass, SourceRows: 10}}}}
	if err := purgeRefusal(ok, "shop"); err != nil {
		t.Errorf("verified backup for the same database must be allowed: %v", err)
	}
	unverified := ok
	unverified.VerifiedAt = ""
	if err := purgeRefusal(unverified, "shop"); err == nil || !strings.Contains(err.Error(), "verify") {
		t.Errorf("unverified backup must be refused with a hint to verify: %v", err)
	}
	if err := purgeRefusal(ok, "other"); err == nil || !strings.Contains(err.Error(), "other") {
		t.Errorf("a different database must be refused naming it: %v", err)
	}
	failed := ok
	failed.Tables = []TableEntry{{Name: "t", Verified: &TableVerification{Status: verifyCountMismatch}}}
	if err := purgeRefusal(failed, "shop"); err == nil {
		t.Error("a table that failed verification must be refused even if verified_at is set")
	}
}

func TestPurgePlanRows(t *testing.T) {
	m := Manifest{Tables: []TableEntry{
		{Name: "a", Verified: &TableVerification{SourceRows: 10}},
		{Name: "b", Verified: &TableVerification{SourceRows: 3}},
	}}
	if got := plannedPurgeRows(m); got != 13 {
		t.Errorf("planned rows = %d, want 13", got)
	}
}
