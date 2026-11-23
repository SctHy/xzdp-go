package user

import "testing"

func TestSignCounts(t *testing.T) {
	for _, tc := range []struct {
		name                string
		bitmap              []byte
		today, days, streak int
		signed              bool
	}{
		{"empty", nil, 1, 0, 0, false},
		{"month start", []byte{0xff}, 1, 1, 1, true},
		{"future bits", []byte{0x7f}, 1, 0, 0, false},
		{"cross byte", []byte{0x83, 0xff}, 10, 5, 4, true},
		{"today missing", []byte{0x0f, 0x80}, 10, 5, 5, false},
		{"short bitmap", []byte{0xff}, 9, 8, 8, false},
		{"two missing days", []byte{0xff}, 10, 8, 0, false},
		{"full month", []byte{0xff, 0xff, 0xff, 0xff}, 31, 31, 31, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			days, streak, signed := signCounts(tc.bitmap, tc.today)
			if days != tc.days || streak != tc.streak || signed != tc.signed {
				t.Fatalf("got (%d,%d,%v), want (%d,%d,%v)", days, streak, signed, tc.days, tc.streak, tc.signed)
			}
		})
	}
}
