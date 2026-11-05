package archive

import "time"

// timeValue interprets a scanned column value as a timestamp. It returns the
// zero time when the value is not a time or a parseable time string.
func timeValue(v interface{}) time.Time {
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		t, _ := parseAnyTime(val)
		return t
	case []byte:
		t, _ := parseAnyTime(string(val))
		return t
	}
	return time.Time{}
}

// firstTimeColumn returns the first column whose value in row looks like a timestamp.
func firstTimeColumn(columns []string, row []interface{}) string {
	for i, v := range row {
		if i < len(columns) && !timeValue(v).IsZero() {
			return columns[i]
		}
	}
	return ""
}

// lastTimeColumn returns the last column whose value in row looks like a timestamp.
func lastTimeColumn(columns []string, row []interface{}) string {
	found := ""
	for i, v := range row {
		if i < len(columns) && !timeValue(v).IsZero() {
			found = columns[i]
		}
	}
	return found
}
