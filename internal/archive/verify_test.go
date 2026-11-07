package archive

import (
	"testing"
)

func TestJudgeVerification(t *testing.T) {
	cases := []struct {
		name             string
		srcRows, arcRows int64
		srcSum, arcSum   string
		want             string
	}{
		{"pass", 10, 10, "a", "a", verifyPass},
		{"count mismatch beats checksum", 10, 9, "a", "a", verifyCountMismatch},
		{"checksum mismatch", 10, 10, "a", "b", verifyChecksumMismatch},
		{"empty table passes", 0, 0, "0", "0", verifyPass},
	}
	for _, c := range cases {
		if got := judgeVerification(c.srcRows, c.arcRows, c.srcSum, c.arcSum); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestVerificationSummary(t *testing.T) {
	tables := []TableEntry{
		{Name: "a", Verified: &TableVerification{Status: verifyPass}},
		{Name: "b", Verified: &TableVerification{Status: verifyPass}},
	}
	if !allVerified(tables) {
		t.Error("two passes should verify the backup")
	}
	tables[1].Verified.Status = verifyCountMismatch
	if allVerified(tables) {
		t.Error("a mismatch must not verify the backup")
	}
	if allVerified(append(tables[:1:1], TableEntry{Name: "c"})) {
		t.Error("an unverified table must not verify the backup")
	}
	if allVerified(nil) {
		t.Error("no tables is not a verified backup")
	}
}

func TestVerificationStateOfManifest(t *testing.T) {
	m := Manifest{Tables: []TableEntry{{Name: "a"}}}
	if got := verificationState(m); got != "no" {
		t.Errorf("unverified: %q", got)
	}
	m.Tables[0].Verified = &TableVerification{Status: verifyPass}
	m.Tables = append(m.Tables, TableEntry{Name: "b"})
	if got := verificationState(m); got != "partial" {
		t.Errorf("partial: %q", got)
	}
	m.VerifiedAt = "2026-01-01T00:00:00Z"
	if got := verificationState(m); got != "yes" {
		t.Errorf("verified: %q", got)
	}
}

func TestSelectColumnsSQL(t *testing.T) {
	if got := selectColumnsSQL("postgres", "t", []string{"id", "ts"}, "x > 0"); got != `SELECT "id", "ts" FROM "t" WHERE x > 0` {
		t.Errorf("pg: %s", got)
	}
	if got := selectColumnsSQL("mysql", "t", []string{"id"}, ""); got != "SELECT `id` FROM `t`" {
		t.Errorf("mysql: %s", got)
	}
	if got := selectColumnsSQL("mysql", "t", nil, ""); got != "SELECT * FROM `t`" {
		t.Errorf("no columns recorded falls back to *: %s", got)
	}
}
