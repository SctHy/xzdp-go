ALTER TABLE tb_voucher_order ADD INDEX ix_order_expiry(status,create_time,id);
ALTER TABLE tb_follow ADD INDEX ix_fans(follow_user_id,user_id);
ALTER TABLE tb_follow ADD INDEX ix_following(user_id,follow_user_id);
