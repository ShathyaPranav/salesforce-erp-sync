package salesforce

import (
	"fmt"
	"time"
)

// wireLayout is how the REST API returns datetimes: 2026-09-15T09:00:00.000+0000.
// Note the offset has no colon, so time.RFC3339 cannot parse it.
const wireLayout = "2006-01-02T15:04:05.000-0700"

// literalLayout is how Relay writes datetimes into SOQL: unquoted, whole
// seconds, UTC "Z". Using Z avoids a '+' that would need URL-encoding.
const literalLayout = "2006-01-02T15:04:05Z"

// ParseDateTime parses a datetime as returned by the REST API. It also
// accepts RFC 3339 so values Relay wrote itself round-trip.
func ParseDateTime(s string) (time.Time, error) {
	if t, err := time.Parse(wireLayout, s); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("not a Salesforce datetime: %q", s)
	}
	return t.UTC(), nil
}

// Literal formats t as a SOQL datetime literal.
func Literal(t time.Time) string {
	return t.UTC().Format(literalLayout)
}
