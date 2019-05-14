DROP TABLE IF EXISTS warehouse;

-- This schema is optimized for ~128 warehouses/partition in terms of
-- hash index bucket counts.

CREATE TABLE warehouse (
    w_id        INT NOT NULL,
    w_name      VARCHAR(10), 
    w_street_1  VARCHAR(20), 
    w_street_2  VARCHAR(20), 
    w_city      VARCHAR(20), 
    w_state     CHAR(2), 
    w_zip       CHAR(9), 
    w_tax       DECIMAL(4,2), 
    w_ytd       DECIMAL(12,2),

    -- 512 is the minimum bucket count
    PRIMARY KEY USING HASH (w_id) BUCKET_COUNT = 512,
    SHARD KEY (w_id)

);

DROP TABLE IF EXISTS district;

CREATE TABLE district (
    d_id        tinyint not null, 
    d_w_id      INT NOT NULL, 
    d_name      varchar(10), 
    d_street_1  varchar(20), 
    d_street_2  varchar(20), 
    d_city      varchar(20), 
    d_state     char(2), 
    d_zip       char(9), 
    d_tax       decimal(4,2), 
    d_ytd       decimal(12,2), 
    d_next_o_id int,
    
    PRIMARY KEY USING HASH (d_w_id, d_id) BUCKET_COUNT = 2048,
    SHARD KEY (d_w_id)
);

DROP TABLE IF EXISTS customer;

CREATE TABLE customer (
    c_id            int not null, 
    c_d_id          tinyint not null,
    c_w_id          INT NOT NULL, 
    c_first         varchar(16), 
    c_middle        char(2), 
    c_last          varchar(16), 
    c_street_1      varchar(20), 
    c_street_2      varchar(20), 
    c_city          varchar(20), 
    c_state         char(2), 
    c_zip           char(9), 
    c_phone         char(16), 
    c_since         datetime, 
    c_credit        char(2), 
    c_credit_lim    bigint, 
    c_discount      decimal(4,2), 
    c_balance       decimal(12,2), 
    c_ytd_payment   decimal(12,2), 
    c_payment_cnt   smallint, 
    c_delivery_cnt  smallint, 
    c_data          text,

    PRIMARY KEY USING HASH (c_w_id, c_d_id, c_id) BUCKET_COUNT = 1048576,
    SHARD KEY(c_w_id),

    INDEX idx_customer (c_w_id,c_d_id,c_last,c_first)
);

drop table if exists history;

create table history (
    h_c_id int, 
    h_c_d_id tinyint, 
    h_c_w_id INT,
    h_d_id tinyint,
    h_w_id INT,
    h_date datetime,
    h_amount decimal(6,2), 
    h_data varchar(24),

    KEY() USING CLUSTERED COLUMNSTORE,
    shard key (h_c_w_id)
);

DROP TABLE IF EXISTS new_orders;

CREATE TABLE new_orders (
    no_o_id INT NOT NULL,
    no_d_id TINYINT NOT NULL,
    no_w_id INT NOT NULL,

    PRIMARY KEY (no_w_id, no_d_id, no_o_id),
    SHARD KEY (no_w_id)
);
    
drop table if exists orders;
    
create table orders (
    o_id int not null, 
    o_d_id tinyint not null, 
    o_w_id INT not null,
    o_c_id int,
    o_entry_d datetime,
    o_carrier_id tinyint,
    o_ol_cnt tinyint, 
    o_all_local tinyint,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    shard key(o_w_id)
);
    
drop table if exists order_line;

create table order_line (
    ol_o_id int not null, 
    ol_d_id tinyint not null,
    ol_w_id INT not null,
    ol_number tinyint not null,
    ol_i_id int, 
    ol_supply_w_id INT,
    ol_delivery_d datetime, 
    ol_quantity tinyint, 
    ol_amount decimal(6,2), 
    ol_dist_info char(24),

    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number),
    SHARD KEY (ol_w_id),

    INDEX ol USING HASH (ol_w_id, ol_d_id, ol_o_id)
);
    
DROP TABLE IF EXISTS item;
    
CREATE REFERENCE TABLE item (
    i_id    INT NOT NULL, 
    i_im_id INT, 
    i_name  VARCHAR(24), 
    i_price DECIMAL(5,2), 
    i_data  VARCHAR(50),

    -- Reference table, not split by partition
    PRIMARY KEY USING HASH (i_id) BUCKET_COUNT = 131072
);
    
DROP TABLE IF EXISTS stock;
    
CREATE TABLE stock (
    s_i_id          INT NOT NULL, 
    s_w_id          INT NOT NULL, 
    s_quantity      SMALLINT, 
    s_dist_01       CHAR(24), 
    s_dist_02       CHAR(24),
    s_dist_03       CHAR(24),
    s_dist_04       CHAR(24), 
    s_dist_05       CHAR(24), 
    s_dist_06       CHAR(24), 
    s_dist_07       CHAR(24), 
    s_dist_08       CHAR(24), 
    s_dist_09       CHAR(24), 
    s_dist_10       CHAR(24), 
    s_ytd           DECIMAL(8,0), 
    s_order_cnt     SMALLINT, 
    s_remote_cnt    SMALLINT,
    s_data          VARCHAR(50),
    
    PRIMARY KEY USING HASH (s_w_id, s_i_id) BUCKET_COUNT = 4193404,
    SHARD KEY (s_w_id),

    INDEX low_stock (s_w_id, s_i_id, s_quantity)
);

CREATE INDEX idx_orders ON orders (o_w_id,o_d_id,o_c_id,o_id);
CREATE INDEX fkey_stock_2 ON stock (s_i_id);
CREATE INDEX fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);
