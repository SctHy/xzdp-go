SET NAMES utf8mb4;
CREATE TABLE bench_slot(id BIGINT PRIMARY KEY, remaining INT NOT NULL);
INSERT INTO bench_slot VALUES(1,100);
INSERT INTO tb_blog(shop_id,user_id,title,images,content,liked) VALUES(1,2,'unrated','','',NULL);
CREATE TABLE bench_receipt(order_id bigint PRIMARY KEY,created_at timestamp DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE bench_order(id bigint PRIMARY KEY,user_id bigint NOT NULL,voucher_id bigint NOT NULL,status int NOT NULL,created_at timestamp DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id,voucher_id),INDEX(status,created_at,id));
CREATE TABLE bench_notice(id bigint AUTO_INCREMENT PRIMARY KEY,event_key varchar(128) NOT NULL UNIQUE,user_id bigint NOT NULL,content varchar(255),pushed boolean DEFAULT FALSE,created_at timestamp DEFAULT CURRENT_TIMESTAMP,INDEX(user_id,id),INDEX(pushed,id));

ALTER TABLE tb_user MODIFY phone varchar(11) NULL;
ALTER TABLE tb_blog ADD COLUMN like_version bigint NOT NULL DEFAULT 0;
