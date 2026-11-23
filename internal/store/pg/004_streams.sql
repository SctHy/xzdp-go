-- PostGIS reads the indexed geography directly. The old geo-index relay has no consumer.
DROP TRIGGER IF EXISTS track_shop_geo ON tb_shop;

CREATE TABLE note_like (
    note_id bigint REFERENCES tb_blog(id) ON DELETE CASCADE,
    user_id bigint NOT NULL,
    PRIMARY KEY (note_id, user_id)
);

-- Previous versions allowed concurrent duplicate follows. Keep the earliest relationship.
DELETE FROM tb_follow f USING tb_follow earlier
WHERE f.user_id = earlier.user_id
  AND f.follow_user_id = earlier.follow_user_id
  AND f.id > earlier.id;
CREATE UNIQUE INDEX follow_pair ON tb_follow(user_id, follow_user_id);

ALTER TABLE tb_voucher_order ADD COLUMN bucket integer NOT NULL DEFAULT 0;

CREATE TABLE ordinary_grant (
    user_id bigint NOT NULL,
    voucher_id bigint NOT NULL,
    PRIMARY KEY (user_id, voucher_id)
);
