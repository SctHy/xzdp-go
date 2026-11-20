ALTER TABLE tb_blog ADD COLUMN like_version BIGINT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS feed_publication(blog_id BIGINT PRIMARY KEY,author_id BIGINT NOT NULL,published BIGINT NOT NULL,pull BOOLEAN NOT NULL,delivered BOOLEAN NOT NULL DEFAULT FALSE,KEY fanout(pull,delivered,blog_id),KEY timeline(author_id,published,blog_id));
