package trade

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	api "xzdp/internal/httpapi/endpoint"
)

const paid = 2
const used = 3

type Payment struct {
	Status string `json:"status"`
	Code   string `json:"code"`
}

func (s Sale) Pay(ctx context.Context, id, buyer int64) (Payment, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Payment{}, err
	}
	defer tx.Rollback()
	var status int
	if err = tx.QueryRowContext(ctx, "SELECT status FROM tb_voucher_order WHERE id=$1 AND user_id=$2 FOR UPDATE", id, buyer).Scan(&status); err != nil {
		return Payment{}, err
	}
	var code string
	switch status {
	case paid:
		err = tx.QueryRowContext(ctx, "SELECT code FROM voucher_redemption WHERE order_id=$1", id).Scan(&code)
	case unpaid:
		// Check database time after acquiring the same row lock used by cancellation.
		result, e := tx.ExecContext(ctx, "UPDATE tb_voucher_order SET status=$1,pay_time=NOW() WHERE id=$2 AND create_time>statement_timestamp()-interval '15 minutes'", paid, id)
		if e != nil {
			return Payment{}, e
		}
		n, e := result.RowsAffected()
		if e != nil {
			return Payment{}, e
		}
		if n == 0 {
			return Payment{}, api.Problem("订单已超过付款期限")
		}
		var random [16]byte
		if _, err = rand.Read(random[:]); err != nil {
			return Payment{}, err
		}
		code = hex.EncodeToString(random[:])
		_, err = tx.ExecContext(ctx, "INSERT INTO voucher_redemption(order_id,code) VALUES($1,$2)", id, code)
	default:
		return Payment{}, api.Problem("当前订单状态不可付款")
	}
	if err != nil {
		return Payment{}, err
	}
	if err = tx.Commit(); err != nil {
		return Payment{}, err
	}
	return Payment{"paid", code}, nil
}

func (s Sale) Redeem(ctx context.Context, id, staff int64, code string) error {
	if len(code) != 32 {
		return api.Problem("核销码格式错误")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE tb_voucher_order o SET status=$3,use_time=statement_timestamp()
 FROM tb_voucher v JOIN shop_staff st ON st.shop_id=v.shop_id AND st.user_id=$1,voucher_redemption r
 WHERE v.id=o.voucher_id AND r.order_id=o.id AND r.code=$2 AND o.id=$4 AND o.status=$5`, staff, code, used, id, paid)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return api.Problem("请检查门店权限、核销码和订单状态")
	}
	if _, err = tx.ExecContext(ctx, "UPDATE voucher_redemption SET redeemed_by=$1 WHERE order_id=$2", staff, id); err != nil {
		return err
	}
	return tx.Commit()
}
