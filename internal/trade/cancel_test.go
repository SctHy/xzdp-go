package trade

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	api "xzdp/internal/httpapi/endpoint"
)

func TestCancelTransitions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   error
	}{
		{"unpaid releases stock", unpaid, nil},
		{"cancelled replay", 4, nil},
		{"paid", paid, api.Problem("只可取消未支付订单")},
		{"redeemed", used, api.Problem("只可取消未支付订单")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT voucher_id,status FROM tb_voucher_order").WithArgs(7, 11).WillReturnRows(sqlmock.NewRows([]string{"voucher_id", "status"}).AddRow(5, tc.status))
			if tc.status == unpaid {
				mock.ExpectExec("UPDATE tb_voucher_order SET status=4").WithArgs(7).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("UPDATE tb_seckill_voucher SET stock=stock").WithArgs(5).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO trade_release").WithArgs(7, 5).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			if tc.want == nil {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			s := Sale{DB: db}
			if got := s.Cancel(t.Context(), 7, 11); got != tc.want {
				t.Fatalf("Cancel = %v, want %v", got, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
