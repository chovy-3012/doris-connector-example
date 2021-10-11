create table test.test_batch
(
    record_id  varchar(255),
    product_id varchar(255),
    rec        varchar(10000),
    client_ip  varchar(255),
    timestamp  datetime
) duplicate key (record_id,product_id) distributed by hash(record_id) buckets 10;


create table test.test_stream
(
    record_id  varchar(255),
    product_id varchar(255),
    rec        varchar(10000),
    client_ip  varchar(255),
    timestamp  datetime
) duplicate key (record_id,product_id) distributed by hash(record_id) buckets 10;