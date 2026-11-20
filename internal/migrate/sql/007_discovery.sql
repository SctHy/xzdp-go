CREATE TABLE IF NOT EXISTS booking_slot(id BIGINT PRIMARY KEY AUTO_INCREMENT, shop_id BIGINT NOT NULL, starts_at DATETIME NOT NULL, ends_at DATETIME NOT NULL, capacity INT NOT NULL, remaining INT NOT NULL, UNIQUE(shop_id,starts_at));
CREATE TABLE IF NOT EXISTS booking(id BIGINT PRIMARY KEY AUTO_INCREMENT, slot_id BIGINT NOT NULL, user_id BIGINT NOT NULL, party_size INT NOT NULL, request_key VARBINARY(64) NOT NULL, cancelled_at DATETIME NULL, UNIQUE(user_id,request_key));
CREATE TABLE IF NOT EXISTS sign_reward_rule(month_start DATE PRIMARY KEY, days_required INT NOT NULL, voucher_id BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS sign_reward_grant(user_id BIGINT NOT NULL, month_start DATE NOT NULL, voucher_id BIGINT NOT NULL, granted_at DATETIME NOT NULL, PRIMARY KEY(user_id,month_start));
CREATE TABLE IF NOT EXISTS user_email(user_id BIGINT PRIMARY KEY, email VARCHAR(254) NOT NULL UNIQUE);
ALTER TABLE tb_user MODIFY phone VARCHAR(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL;
