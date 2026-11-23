CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE INDEX shop_category_location ON tb_shop USING gist(type_id,location);
