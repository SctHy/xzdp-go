CREATE TABLE IF NOT EXISTS voucher_redemption (
    order_id BIGINT PRIMARY KEY,
    code CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    redeemed_by BIGINT NULL
);
CREATE TABLE IF NOT EXISTS shop_staff (
    shop_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    PRIMARY KEY (shop_id, user_id)
);
