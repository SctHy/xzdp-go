-- One database round trip. A failure rolls back the receipt as well as the stock.
CREATE FUNCTION persist_voucher_order(oid bigint, buyer bigint, voucher bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO trade_receipt(order_id) VALUES(oid) ON CONFLICT(order_id) DO NOTHING;
    IF NOT FOUND THEN RETURN; END IF;
    INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES(oid,buyer,voucher,1);
    UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=voucher AND stock>0;
    IF NOT FOUND THEN RAISE EXCEPTION 'database stock exhausted'; END IF;
    INSERT INTO trade_notice(event_key,user_id,content)
        VALUES('order:' || oid,buyer,'优惠券订单 ' || oid || ' 已生成');
END $$;
