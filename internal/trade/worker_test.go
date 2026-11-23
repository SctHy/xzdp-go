package trade

import (
	"math"
	"strconv"
	"testing"
)

func TestKafkaEventValidation(t *testing.T) {
	valid := map[string]string{"id": "9007199254740993", "voucher": "1", "user": "5", "bucket": "63"}
	v, e := parse(valid)
	if e != nil || v.id != 9007199254740993 {
		t.Fatal(v, e)
	}
	for _, field := range []string{"bucket", "user", "voucher", "id"} {
		for _, bad := range []string{"-1", "x", ""} {
			values := map[string]string{}
			for k, v := range valid {
				values[k] = v
			}
			values[field] = bad
			if _, e = parse(values); e == nil {
				t.Fatalf("accepted %s=%q", field, bad)
			}
		}
	}
}

func TestParseBoundaries(t *testing.T) {
	for _, bucket := range []string{"0", "63"} {
		values := map[string]string{"id": "9223372036854775807", "user": "1", "voucher": "2", "bucket": bucket}
		evt, err := parse(values)
		if err != nil || evt.id != math.MaxInt64 || evt.user != 1 || evt.voucher != 2 || strconv.Itoa(evt.bucket) != bucket {
			t.Fatal(evt, err)
		}
	}
	for _, field := range []string{"id", "user", "voucher", "bucket"} {
		for _, value := range []string{"", "9223372036854775808", "1.5", "0"} {
			m := map[string]string{"id": "1", "user": "1", "voucher": "1", "bucket": "0"}
			switch {
			case value == "":
				delete(m, field)
			case field == "bucket" && value == "0":
				m[field] = "64"
			default:
				m[field] = value
			}
			if _, err := parse(m); err == nil {
				t.Fatalf("accepted %v", m)
			}
		}
	}
}
