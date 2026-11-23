ALTER TABLE trade_dead_letter DROP CONSTRAINT trade_dead_letter_pkey,
    ALTER COLUMN order_id DROP NOT NULL,
    ADD COLUMN source text UNIQUE,
    ADD COLUMN payload bytea;
CREATE UNIQUE INDEX trade_dead_letter_order ON trade_dead_letter(order_id);
