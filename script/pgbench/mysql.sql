SET NAMES utf8mb4;
SET SESSION cte_max_recursion_depth=24000;
INSERT INTO tb_shop (name,type_id,images,address,x,y,sold,comments,score)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<24000)
SELECT CONCAT(IF(n%3=0,'街角咖啡馆','社区食堂'),n),1+MOD(n,7),'','fixture',
  121.4+(MOD(n*7919,24001)-12000)/48000,
  31.2+(MOD(n*6151,24001)-12000)/48000,0,0,45 FROM seq;
INSERT INTO tb_blog (shop_id,user_id,title,images,content,liked,comments)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<12000)
SELECT 1,2,CONCAT('note ',n),'',REPEAT('text ',48),MOD(n*997,13007),0 FROM seq;
CREATE TABLE bench_slot(id BIGINT PRIMARY KEY, remaining INT NOT NULL);
INSERT INTO bench_slot VALUES(1,100);
INSERT INTO tb_blog(shop_id,user_id,title,images,content,liked) VALUES(1,2,'unrated','','',NULL);
CREATE TABLE bench_receipt(order_id bigint PRIMARY KEY,created_at timestamp DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE bench_order(id bigint PRIMARY KEY,user_id bigint NOT NULL,voucher_id bigint NOT NULL,status int NOT NULL,created_at timestamp DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id,voucher_id),INDEX(status,created_at,id));
CREATE TABLE bench_notice(id bigint AUTO_INCREMENT PRIMARY KEY,event_key varchar(128) NOT NULL UNIQUE,user_id bigint NOT NULL,content varchar(255),pushed boolean DEFAULT FALSE,created_at timestamp DEFAULT CURRENT_TIMESTAMP,INDEX(user_id,id),INDEX(pushed,id));

ALTER TABLE tb_user MODIFY phone varchar(11) NULL;
ALTER TABLE tb_blog ADD COLUMN like_version bigint NOT NULL DEFAULT 0;
