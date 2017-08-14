CREATE TABLE sephora_product (
       id VARCHAR(100),
       product_url VARCHAR(2000),
       sku INTEGER,
       category VARCHAR(200),
       brand VARCHAR(100),
       name VARCHAR(1000),
       rating DECIMAL,
       detail_text TEXT,
       size_oz FLOAT,
       price INT,
       PRIMARY KEY(id)
);

CREATE INDEX sephora_product_rating_idx ON sephora_product (rating);
CREATE INDEX sephora_product_category_idx ON sephora_product (category);
CREATE INDEX sephora_product_detail_text ON sephora_product USING gin(to_tsvector('english', 'detail_text'));

CREATE TABLE sephora_product_review (
       product_id VARCHAR(100) NOT NULL,
       review_title VARCHAR(5000) NOT NULL,
       review_text TEXT NOT NULL,
       rating SMALLINT NOT NULL,
       age_range VARCHAR(50),
       skin_type VARCHAR(50),
       skin_tone VARCHAR(50),
       eye_color VARCHAR(50),
       reviewer_username VARCHAR(300),
       tags VARCHAR(5000)[]
);

CREATE INDEX sephora_product_review_product_id_idx ON sephora_product_review (product_id);
CREATE INDEX sephora_product_review_tags_idx on sephora_product_review USING GIN ("tags");
