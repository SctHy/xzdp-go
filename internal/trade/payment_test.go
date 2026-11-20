package trade

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/stretchr/testify/require"
)

func TestMockCheckoutRoutesRequireOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := server.New()
		Sale{}.Register(h, enabled)
		want := 404
		if enabled {
			want = 401
		}
		for _, path := range []string{"/voucher-order/pay/1", "/voucher-order/redeem/1"} {
			require.Equal(t, want, ut.PerformRequest(h.Engine, "POST", path, nil).Result().StatusCode())
		}
	}
}

func TestPayRollsBackWhenCodeCannotBeStored(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	failure := errors.New("storage failure")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT status .* FOR UPDATE").WithArgs(1, 2).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(unpaid))
	mock.ExpectExec("UPDATE tb_voucher_order").WithArgs(paid, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO voucher_redemption").WithArgs(1, sqlmock.AnyArg()).WillReturnError(failure)
	mock.ExpectRollback()
	result, err := (Sale{DB: db}).Pay(context.Background(), 1, 2)
	require.ErrorIs(t, err, failure)
	require.Empty(t, result.Code)
	require.NoError(t, mock.ExpectationsWereMet())
}
