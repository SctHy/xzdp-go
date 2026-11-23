INSERT INTO tb_shop_type(id,name) VALUES(1,'咖啡'),(2,'餐厅'),(3,'书店');
INSERT INTO tb_shop(id,name,type_id,images,address,x,y,sold,comments,score,open_hours) VALUES
(1,'山隙咖啡',1,'','南山路 18 号',120.15,30.25,0,0,48,'09:00–20:00'),
(2,'春山食堂',2,'','柳浪巷 7 号',120.153,30.252,0,0,47,'11:00–21:00'),
(3,'白塔书房',3,'','河坊街 32 号',120.159,30.249,0,0,49,'10:00–22:00');
INSERT INTO booking_slot(shop_id,starts_at,ends_at,capacity,remaining) SELECT id,current_date+interval '2 days 10 hours',current_date+interval '2 days 12 hours',12,12 FROM tb_shop;
INSERT INTO tb_voucher(id,shop_id,title,pay_value,actual_value,type) VALUES(1,1,'咖啡抵用券',0,1500,0),(2,2,'春山双人晚餐',6800,12800,1);
INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES(2,100,now(),now()+interval '1 year');
SELECT setval(pg_get_serial_sequence('tb_shop','id'),3),setval(pg_get_serial_sequence('tb_voucher','id'),2),setval(pg_get_serial_sequence('tb_shop_type','id'),3);
INSERT INTO tb_user(nick_name) VALUES('春山店员');
INSERT INTO user_email(user_id,email) SELECT id,'staff@example.test' FROM tb_user WHERE nick_name='春山店员';
INSERT INTO shop_staff(shop_id,user_id) SELECT 2,user_id FROM user_email WHERE email='staff@example.test';
INSERT INTO tb_blog(shop_id,user_id,title,images,content) SELECT 2,user_id,'今天的春山食堂','','从柳浪巷转进来，靠窗的一排座位适合两人午餐。' FROM user_email WHERE email='staff@example.test';
